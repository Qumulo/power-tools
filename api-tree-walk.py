import qtreewalk


def do_per_file(ent, d, out_file=None, rc=None):
    """ent: attribute JSON
       d: parent directory attributes (d['path'] for the path)
       out_file: optional file handle for logging
       rc: RestClient instance suitable for modifying data or metadata
    This is where we define what happens to each file encountered.
    The default behavior is to print and log file info.
    """
    # here are all potential file attributes that are part of the *ent* JSON
    # search/track/filter as desired
    """
     'path': '/tweets/snow/tweets-snow-2019011902.jsonline',
     'name': 'tweets-snow-2019011902.jsonline',
     'change_time': '2019-01-19T10:55:47.591487372Z',
     'creation_time': '2019-01-19T09:56:01.10147995Z',
     'modification_time': '2019-01-19T10:55:47.591487372Z',
     'child_count': 0,
     'extended_attributes': {'archive': True,
                             'compressed': False,
                             'hidden': False,
                             'not_content_indexed': False,
                             'read_only': False,
                             'system': False,
                             'temporary': False},
     'id': '11041240942',
     'group': '17179869184',
     'group_details': {'id_type': 'NFS_GID', 'id_value': '0'},
     'owner': '12884901888',
     'owner_details': {'id_type': 'NFS_UID', 'id_value': '0'},
     'mode': '0644',
     'num_links': 1,
     'size': '34342740',
     'symlink_target_type': 'FS_FILE_TYPE_UNKNOWN',
     'type': 'FS_FILE_TYPE_FILE'
    """
    file_info = "%s%s\t%s\t%s" % \
                (d['path'], ent['name'], ent['size'], ent['type'])
    # print(file_info)
    if out_file:
        out_file.write(file_info + '\n')

# Replace the default behavior with the behavior defined above
qtreewalk.do_per_file = do_per_file

if __name__ == '__main__':
    args = qtreewalk.parse_args()
    qtreewalk.walk_tree(args.s, "admin", args.p, args.d)
