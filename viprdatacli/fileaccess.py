import sys
import os
import time
import subprocess
import xml2obj as x2o
import datetime
import json
import calendar
import re
from viprdata.viprdata import ViprData, FILE_ACCESS_MODE_HEADER, \
  FILE_ACCESS_DURATION_HEADER, FILE_ACCESS_HOST_LIST_HEADER, \
  FILE_ACCESS_USER_HEADER, FILE_ACCESS_START_TOKEN_HEADER, \
  FILE_ACCESS_END_TOKEN_HEADER, S3_PORT, SWIFT_PORT

NFS_PORT = 2049

class ViprFileAccess(object):
    def __init__(self, api, endpoint, key, secret):
        self.api = api
        self.endpoint = endpoint
        self.key = key
        self.secret = secret
        self.bourne = ViprData()

        if self.api == "s3":
            self.fileaccess_ops = {"target": "s3",
                                   "getAccessMode": self.bourne.bucket_switchget,
                                   'switchAccessMode': self.bourne.bucket_switch,
                                   'getFileList': self.bourne.bucket_fileaccesslist}
            self.bourne.connect(self.endpoint, S3_PORT)
        elif self.api == "swift":
            self.fileaccess_ops = {"target": "swift",
                                   "getAccessMode": self.bourne.container_getaccessmode,
                                   'switchAccessMode': self.bourne.container_switchfileaccess,
                                   'getFileList': self.bourne.container_getfileaccess}
            self.bourne.connect(self.endpoint, SWIFT_PORT)
    
    def _get_bucket_mode(self, namespace, bucket):
        response = self.fileaccess_ops["getAccessMode"](namespace, bucket, self.key, self.secret)
        return self._build_bucket_mode_result(response.headers)

    def _set_bucket_mode(self, namespace, bucket, mode, hosts, duration, token, uid):
        response = self.fileaccess_ops["switchAccessMode"](namespace, bucket, mode, hosts, duration, token, uid, self.key, self.secret)
        return self._build_bucket_mode_result(response.headers)
        
    # wait until the mode is pesent
    def _wait_for(self, namespace, bucket, mode):
        print 'waiting for bucket %s to be in mode %s' % (bucket, mode)
        while True:
            response = self.fileaccess_ops["getAccessMode"](namespace, bucket, self.key, self.secret)
            h = response.headers
            if (h[FILE_ACCESS_MODE_HEADER] and h[FILE_ACCESS_MODE_HEADER].lower() == mode.lower()):
                # print 'bucket state: ' + mode
                break
            # print 'sleep 2s'
            time.sleep(2)
        print 'switched to ' + mode
        
    def _get_bucket_access(self, namespace, bucket):
        falist = self.fileaccess_ops["getFileList"](namespace, bucket, self.key, self.secret)
        xml = x2o.xml2obj(falist.text.strip().encode('ascii', 'ignore'))
        return xml['fileaccess_response']
    
    def _build_bucket_mode_result(self, headers):
        good_keys = [FILE_ACCESS_MODE_HEADER, FILE_ACCESS_DURATION_HEADER, FILE_ACCESS_HOST_LIST_HEADER,
                     FILE_ACCESS_USER_HEADER, FILE_ACCESS_START_TOKEN_HEADER, FILE_ACCESS_END_TOKEN_HEADER]
        result = {}
        for key in good_keys:
            if key in headers: result[key.replace("x-emc-file-access-", "")] = headers[key]
        return result

class ViprMount(ViprFileAccess):
    def __init__(self, api, endpoint, key, secret, namespace, bucket, token, hosts, readonly, uid, duration, parent_dir):
        super(ViprMount, self).__init__(api, endpoint, key, secret)
        self.namespace = namespace
        self.bucket = bucket
        self.token = token
        self.hosts = hosts
        self.readonly = readonly
        self.uid = str(uid)
        self.duration = duration
        self.parent_dir = parent_dir
        
    def execute(self):
        print 'exporting %s:%s/%s to %s:%s as %s' % (self.key, self.namespace or '', self.bucket, self.hosts, self.parent_dir, self.uid)
        if (self.token):
            print 'with token ' + self.token
        if (self.readonly):
            print 'read-only'
            mode = 'readOnly'
        else:
            mode = 'readWrite'
        duration = str(int(self.duration) * 60)  # convert to seconds
    
        # enable fs access
        self._set_bucket_mode(self.namespace, self.bucket, mode, self.hosts, duration, self.token, self.uid)
        self._wait_for(self.namespace, self.bucket, mode)
        end_token = self._get_end_token()
        
        # get exports
        mounts = {}
        mount_count = 0
        fileaccess = self._get_bucket_access(self.namespace, self.bucket)
    
        # mount all exports
        exports = fileaccess['mountPoints']
        if not type(exports) == list:
            exports = [exports]
        for export in exports:
            mount_count += 1
            mounts[export] = 'mount%d' % mount_count
            if os.name == 'nt':
                mounts[export] = _do_mount(export, '%s/%s' % (self.parent_dir, mounts[export]))
            else:
                _do_mount(export, '%s/%s' % (self.parent_dir, mounts[export]))
        
        # list object locations
        print
        self._print_mounts(fileaccess, mounts)
        
        # write config
        self._write_config(mounts, end_token)
        
        # print token
        print
        print 'token for new objects: %s' % (end_token)
        print
        
    def _get_end_token(self):
        h = self._get_bucket_mode(self.namespace, self.bucket)
        if (h["end-token"]):
            return  h["end-token"]  # TODO return token
        raise RuntimeError("failed, cannot get token")
    
    def _print_mounts(self, fileaccess, mounts):                    
        print '---------------object paths---------------------'
    
        objects = fileaccess['objects']
        if type(objects) == dict:
            objects = [objects]
        for obj in objects:
            export = obj['deviceExport']
            relpath = obj['relativePath']
            if os.name == "nt":
                path = os.path.join(mounts[export], relpath.replace('/','\\'))
            else:
                path = os.path.join('%s/%s' % (self.parent_dir, mounts[export]), relpath)
            # wPath = path.replace("/", "\\")
            print  "%s\t%s" % (obj['name'], path)
            
    def _write_config(self, mounts, token):
        expires = datetime.datetime.now() + datetime.timedelta(seconds=self.duration)
        mount_info = ViprMountInfo(parent_dir=self.parent_dir,
                                   api=self.api,
                                   endpoint=self.endpoint,
                                   key=self.key,
                                   secret=self.secret,
                                   namespace=self.namespace,
                                   bucket=self.bucket,
                                   expires=expires,
                                   token=token,
                                   hosts=self.hosts,
                                   uid=self.uid,
                                   mounts=mounts)
        mount_info.write()
#/ViprMount

class ViprUmount(ViprMount):
    def __init__(self, parent_dir):
        mount_info = ViprMountInfo.read(parent_dir)
        timedelta = mount_info.expires - datetime.datetime.now()
        duration = timedelta.seconds + timedelta.days * 24 * 3600
        super(ViprUmount, self).__init__(mount_info.api, mount_info.endpoint,
                                         mount_info.key, mount_info.secret,
                                         mount_info.namespace, mount_info.bucket,
                                         None, mount_info.hosts,
                                         False, mount_info.uid,
                                         duration, parent_dir)
        self.end_token = mount_info.token
        self.mount_info = mount_info

    def execute(self):
        #unmount all exports
        mounts = self.mount_info.mounts.copy();
        for export in self.mount_info.mounts:
            try:
                if os.name == 'nt':
                    _do_unmount(self.mount_info.mounts[export])
                else:
                    _do_unmount('%s/%s' % (self.parent_dir, self.mount_info.mounts[export]))
                del mounts[export]
            except Exception as e:
                # don't fail the process if unmounting fails (user can fix that)
                print e

        # keep track of mounts so user can call script again on failures
        self._write_config(mounts, self.end_token)
        
        #disable fs access for bucket
        print 'disabling file access for %s:%s/%s' % (self.key, self.namespace, self.bucket)
        if (self.end_token): print 'with token ' + self.end_token
        mode = 'disabled'
        self._set_bucket_mode(self.namespace, self.bucket, mode, self.hosts, None,
                              self.end_token, self.uid)
        # TODO: handle parallel workflows here (might not end up in "disabled")
        #self._wait_for(self.namespace, self.bucket, mode)
        
        # delete config file if successful
        self.mount_info.clean()
#/ViprUmount

class ViprMountInfo:
    @staticmethod
    def read(parent_dir):
        config_file = os.path.join(parent_dir, '.viprmount.json')
        if not os.path.isfile(config_file):
            raise ViprScriptError('%s does not appear to be a ViPR mount location' % parent_dir)
        f = open(config_file, 'r')
        dct = json.loads(f.read(), object_hook=_info_decoder)
        dct['parent_dir'] = parent_dir
        info = ViprMountInfo(**dct)
        f.close()
        return info
    
    def __init__(self, parent_dir, api, endpoint, key, secret, namespace, bucket, expires, token, hosts, uid, mounts):
        self.parent_dir = parent_dir
        self.api = api
        self.endpoint = endpoint
        self.key = key
        self.secret = secret
        self.namespace = namespace
        self.bucket = bucket
        self.expires = expires
        self.token = token
        self.hosts = hosts
        self.uid = uid
        self.mounts = mounts
        self.config_file = os.path.join(parent_dir, '.viprmount.json')
        
    def write(self):
        f = open(self.config_file, 'w')
        f.write(json.dumps(self, cls=ViprMountInfoEncoder))
        f.close()
        
    def clean(self):
        if os.name != 'nt':
            for export in self.mounts:
                os.rmdir('%s/%s' % (self.parent_dir, self.mounts[export]))
        os.remove(self.config_file)
#/ViprMountInfo
    
def _get_peer_facing_ip(peer_address):
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((peer_address, NFS_PORT))
    ip = s.getsockname()[0]
    s.close()
    return ip

def _do_mount(export_point, mount_point):
    if os.name == 'nt':
        mount_point = 'the next drive letter'
    print 'mounting %s to %s' % (export_point, mount_point)
    if os.name == "nt":
        cmd = "mount %s *" % export_point
    else:
        if (os.path.exists(mount_point) == False):
            os.makedirs(mount_point)
        cmd = "mount -t nfs %s %s" % (export_point, mount_point)

    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)

    (out, err) = process.communicate()
    if process.returncode != 0:
        raise IOError("ERROR: failed to mount %s\n%s%s" % (export_point, out, err))

    if os.name == "nt":
        # Windows print a message to indicate where is mounted
        mount_point = os.path.join(out[0:2], os.path.sep)
    
    return mount_point
        
def _do_unmount(mount_point):
    if os.name != 'nt':
        mount_point = os.path.abspath(mount_point)
    print 'unmounting %s' % (mount_point)

    process = subprocess.Popen("umount %s" % mount_point, shell=True, stdout=subprocess.PIPE)
    
    (out, err) = process.communicate()
    if process.returncode != 0:
        raise IOError("ERROR: failed to umount %s\n%s%s" % (mount_point, out, err))

date_regex = re.compile('^new Date\(([0-9]+)\)')
def _info_decoder(dct):
    # Python 2.6 requires str keywords
    dct = dict((key.encode() if isinstance(key, unicode) else key,
                value.encode() if isinstance(value, unicode) else value)
               for (key, value) in dct.items())
    if 'config_file' in dct: del dct['config_file']
    for key in dct:
        if not isinstance(dct[key], basestring): continue
        match = date_regex.match(dct[key])
        if match:
            dct[key] = datetime.datetime.utcfromtimestamp(int(match.group(1)))
    return dct

class ViprMountInfoEncoder(json.JSONEncoder):
    def default(self, info):
        if isinstance(info, ViprMountInfo):
            dct = info.__dict__.copy()
            dct['expires'] = 'new Date(%d)' % calendar.timegm(dct['expires'].timetuple())
            return dct
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, info)

# represents configuration or execution errors (perhaps the script was run incorrectly)
class ViprScriptError(Exception):
    def __init__(self, message):
        self.message = message
        
    def __str__(self):
        return repr(self.message)
