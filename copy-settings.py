#!/usr/bin/env python
# Copyright (c) 2016 Qumulo, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.


# Import python libraries
import sys
import json
import argparse
import qumulo.lib.auth
import qumulo.lib.request
import qumulo.rest

def login(host, port, user, password):
    try:
        conninfo = None
        creds = None

        # Create a connection to the REST server
        conninfo = qumulo.lib.request.Connection(host, int(port))

        # Provide username and password to retreive authentication tokens
        # used by the credentials object
        login_results, _ = qumulo.rest.auth.login(
                conninfo, None, user, password)

        # Create the credentials object which will be used for
        # authenticating rest calls
        creds = qumulo.lib.auth.Credentials.from_login_response(login_results)
        return (conninfo, creds)

    except Exception, excpt:
        print "Error connecting to the REST server: %s" % excpt
        print __doc__
        sys.exit(1)

def clone_groups(src, dest):
    try:
        groups = qumulo.rest.groups.list_groups(src[0], src[1])
        for group in groups[0]:
            add_group(dest, group)

    except qumulo.lib.request.RequestError, excpt:
        print "Error: %s" % excpt


def clone_users(src, dest):
    try:
        users = qumulo.rest.users.list_users(src[0], src[1])
        for user in users[0]:
            add_user(dest, user)

    except qumulo.lib.request.RequestError, excpt:
        print "Error: %s" % excpt

def clone_nfs_exports(src, dest):
    try:
        exports = qumulo.rest.nfs.nfs_list_shares(src[0], src[1])
        for export in exports[0]:
            add_nfs_export(dest, export)

    except qumulo.lib.request.RequestError, excpt:
        print "Error: %s" % excpt

def clone_smb_shares(src, dest):
    try:
        shares = qumulo.rest.smb.smb_list_shares(src[0], src[1])
        for share in shares[0]:
            add_smb_share(dest, share)

    except qumulo.lib.request.RequestError, excpt:
        print "Error: %s" % excpt

def clone_vpn_and_missionq(src, dest):
    try:
        support_config = qumulo.rest.support.get_config(src[0], src[1])
        support_config = json.loads(str(support_config))
        if support_config['vpn_enabled']:
            clone_vpn(src, dest)
        clone_missionq(support_config, dest)
    except qumulo.lib.request.RequestError, excpt:
        print "Error: %s" % excpt

def clone_missionq(support_config, dest):
    try:
        qumulo.rest.support.set_config(
            dest[0],
            dest[1],
            support_config['enabled'],
            support_config['mq_host'],
            support_config['mq_port'],
            support_config['mq_proxy_host'],
            support_config['mq_proxy_port'],
            support_config['s3_proxy_host'],
            support_config['s3_proxy_port'],
            support_config['s3_proxy_disable_https'],
            support_config['period'],
            support_config['vpn_host'],
            support_config['vpn_enabled'])
    except qumulo.lib.request.RequestError, excpt:
        print "Error: %s" % excpt

def clone_vpn(src, dest):
    try:
        vpn_keys = qumulo.rest.support.get_vpn_keys(
            src[0],
            src[1])
        if str(vpn_keys) != "null":
            vpn_config = json.loads(str(vpn_keys))
            qumulo.rest.support.install_vpn_keys(
                dest[0],
                dest[1],
                vpn_config)
            print 'Installed VPN keys'
    except qumulo.lib.request.RequestError, excpt:
        print "Error: %s" % excpt

def add_nfs_export(dest, export):
    try:
        restrictions = []
        for r in export['restrictions']:
            restrictions.append(qumulo.rest.nfs.NFSRestriction(r))

        qumulo.rest.nfs.nfs_add_share(
            dest[0],
            dest[1],
            export['export_path'],
            export['fs_path'],
            export['description'],
            restrictions,
            allow_fs_path_create=True)

    except qumulo.lib.request.RequestError, excpt:
        if (excpt.status_code == 409):
            msg = "NFS Export '" + export['export_path'] + "' already exists on destination..."
        else:
            msg = "Error: %s" % excpt

def add_smb_share(dest, share):
    try:
        qumulo.rest.smb.smb_add_share(
            dest[0],
            dest[1],
            share['share_name'],
            share['fs_path'],
            share['description'],
            share['read_only'],
            share['allow_guest_access'],
            True,  #allow_fs_path_create
            share['access_based_enumeration_enabled'])
    except qumulo.lib.request.RequestError, excpt:
        if (excpt.status_code == 409):
            msg = "SMB Share '" + share['share_name'] + "' already exists on destination..."
        else:
            msg = "Error: %s" % excpt

        print msg

def add_group(dest, group):
    try:
        qumulo.rest.groups.add_group(
            dest[0],
            dest[1],
            group['name'],
            group['gid']
            )
    except qumulo.lib.request.RequestError, excpt:
        if (excpt.status_code == 409):
            msg = "Group '" + group['name'] + "' already exists on destination..."
        else:
            msg = "Error: %s" % excpt

        print msg

def add_user(dest, user):
    try:
        qumulo.rest.users.add_user(
            dest[0],
            dest[1],
            user['name'],
            user['primary_group'],
            user['uid']
            )
    except qumulo.lib.request.RequestError, excpt:
        if (excpt.status_code == 409):
            msg = "User '" + user['name'] + "' already exists on destination..."
        else:
            msg = "Error: %s" % excpt

        print msg

def clone_config(src, dest):
    clone_groups(src, dest)
    print 'Cloned groups'
    clone_users(src, dest)
    print 'Cloned users'
    clone_nfs_exports(src, dest)
    print 'Cloned NFS exports'
    clone_smb_shares(src, dest)
    print 'Cloned SMB shares'
    clone_vpn_and_missionq(src, dest)
    print 'Cloned VPN and MissionQ'

def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--src_host', dest='src_host', required=True, type=str)
    parser.add_argument('--src_port', dest='src_port', default=8000, type=int)
    parser.add_argument('--src_user', dest='src_user', required=True,
                        type=str)
    parser.add_argument('--src_pass', dest='src_pass', required=True,
                        type=str)

    parser.add_argument('--dest_host', dest='dest_host', required=True,
                        type=str)
    parser.add_argument('--dest_port', dest='dest_port', default=8000, type=int)
    parser.add_argument('--dest_user', dest='dest_user', required=True,
                        type=str)
    parser.add_argument('--dest_pass', dest='dest_pass', required=True,
                        type=str)

    opts = parser.parse_args(argv)

    src = login(opts.src_host, opts.src_port, opts.src_user,
                opts.src_pass)
    dest = login(opts.dest_host, opts.dest_port, opts.dest_user,
                 opts.dest_pass)

    print 'Connected to both clusters... About to clone'
    print 'You are about to clone settings on cluster {0} on to cluster {1}'\
        .format(opts.src_host, opts.dest_host)
    print 'Please make sure that the src and destination clusters are correct\n'

    response = raw_input('Press Y to proceed\n')

    if response == 'Y' or response =="y":
        clone_config(src, dest)

# Main
if __name__ == '__main__':
    main(sys.argv[1:])
