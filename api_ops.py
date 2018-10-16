import json

def set_permission(config, ses, file_id):
    perms = {'aces': [{'trustee_details': {'id_type': 'INTERNAL', 'id_value': 'File Owner'}, 
                 'flags': [], 
                 'rights': ['READ', 'READ_EA', 'READ_ATTR', 'READ_ACL', 'WRITE_EA', 
                             'WRITE_ATTR', 'EXECUTE', 'ADD_FILE', 'ADD_SUBDIR', 
                             'DELETE_CHILD', 'SYNCHRONIZE'],
                 'type': 'ALLOWED', 
                 'trustee': '18446744065119617025'
                 }, 
                 {'trustee_details': {'id_type': 'INTERNAL', 'id_value': 'File Group Owner'}, 
                  'flags': [], 
                  'rights': ['READ', 'READ_EA', 'READ_ATTR', 'READ_ACL', 'WRITE_EA',
                             'WRITE_ATTR', 'EXECUTE', 'ADD_FILE', 'ADD_SUBDIR', 
                             'DELETE_CHILD', 'SYNCHRONIZE'],
                 'type': 'ALLOWED', 
                 'trustee': '18446744065119617026'
                 }, 
                 {'trustee_details': {'id_type': 'SMB_SID', 'id_value': 'S-1-1-0'}, 
                  'flags': [], 
                  'rights': ['READ', 'READ_EA', 'READ_ATTR', 'READ_ACL', 'WRITE_EA',
                             'WRITE_ATTR', 'EXECUTE', 'ADD_FILE', u'ADD_SUBDIR', 
                             'DELETE_CHILD', 'SYNCHRONIZE'],
                  'type': 'ALLOWED', 
                  'trustee': '8589934592'}
                  ],
        'posix_special_permissions': [],
        'control': ['PRESENT'],
    }
    url = 'https://%s:8000/v1/files/%s/info/acl' % (
                config.s, 
                file_id)
    resp = ses.get(url, verify=False)
    existing_perms = json.loads(resp.text)
    if len(existing_perms['acl']['aces']) == 0 or perms['aces'][0] != existing_perms['acl']['aces'][0]:
        url = 'https://product:8000/v1/files/%s/info/acl' % file_id
        print("Fixing: %s" % file_id)
        # res = ses.put(url, json.dumps(perms), verify=False)
        # print(res.text)
