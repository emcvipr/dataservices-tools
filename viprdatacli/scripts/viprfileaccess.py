import sys
import os
import getopt
import pprint
import traceback
from viprdatacli.fileaccess import ViprFileAccess, _get_peer_facing_ip, cli_version


def print_exports(xml):
    mount_map = {}
    mounts = xml['mountPoints']
    if type(mounts) != list:
        mounts = [mounts]
    print 'all exports:'
    for mount in mounts:
        print '--%s' % mount
        mount_map[no_unicode(mount)] = []
    for obj in xml['objects']:
        mount_map[no_unicode(obj['deviceExport'])].append({no_unicode(obj['name']): no_unicode(obj['relativePath'])})

    print
    for mount in mount_map:
        print 'objects under export'
        print '--%s:' % mount
        for obj in mount_map[mount]:
            for name in obj:
                print'----object_name: %s' % name
                print'------object_path: %s' % obj[name]
        print


def no_unicode(string):
    if type(string) == unicode:
        return string.encode()
    return string


def cli_help(script_name):
    print 'usage:'
    print '    %s -k <access_key> -s <secret_key> [..] <action> <bucket> [mode]' % script_name
    print 'options:'
    print '    -h                 : print this help text'
    print '    -v                 : print traceback on errors'
    print '    -V                 : print the version of the main library'
    print '    -e <endpoint>      : specify the data node endpoint (required if'
    print '                         the BOURNE_DATA_IPADDR env var is not set)'
    print '    -k <access_key>    : specify your vipr access_key (this key must'
    print '                         own the bucket)'
    print '    -s <secret_key>    : the secret key associated with the access key'
    print '    -n <namespace>     : specify the namespace of the tenant (optional)'
    print '    -t <token>         : specify the token; will only export objects'
    print '                         newer than or disable objects older than the'
    print '                         token'
    print '    -d <minutes>       : specify the duration the export will last'
    print '                         in minutes (defaults to 12 hours)'
    print '    -u <local_uid>     : specify the client uid which should have'
    print '                         access to the mounted files (defaults to the'
    print '                         uid running this script)'
    print '    -H <hosts>         : specify the set of hosts allowed to access'
    print '                         the exports (defaults to the host running'
    print '                         this script)'
    print '    -p                 : preserves the paths of objects that were'
    print '                         originally ingested from an NFS export'
    print '                         * requires ViPR 1.1+'
    print '    action             : the action to perform. can be one of [getmode,'
    print '                         setmode, getaccess]'
    print '    bucket             : the bucket containing the objects to export'
    print '    mode               : the access mode to set for the objects. may'
    print '                         be one of [readOnly, readWrite, disabled].'
    print '                         *required for the setmode action.'
    print ''
    print 'Note: you must call setmode to set the mode of the bucket to'
    print '      readOnly or readWrite before calling getaccess, otherwise'
    print '      getaccess will return an error.'


# main
def main():
    #----------------------------------------------------------------------
    # command-line parsing
    #----------------------------------------------------------------------
    action = mode = endpoint = key = secret = namespace = bucket = token = hosts = preserve = None
    duration = 60 * 12
    if os.name == "nt":
        uid = 1001  # Registry settings are needed to set the anon UID in Windows
    else:
        uid = os.getuid()

    opts, leftover = getopt.getopt(sys.argv[1:], "hvVe:k:s:n:t:d:u:H:p")
    options = dict(opts)

    if "-h" in options:
        cli_help(sys.argv[0])
        exit(0)
    if "-V" in options:
        cli_version(sys.argv[0])
        exit(0)

    if "-e" in options:
        endpoint = options["-e"]
    if "-k" in options:
        key = options["-k"]
    if "-s" in options:
        secret = options["-s"]
    if "-n" in options:
        namespace = options["-n"]
    if "-t" in options:
        token = options["-t"]
    if "-d" in options:
        duration = options["-d"]
    if "-u" in options:
        uid = options["-u"]
    if "-H" in options:
        hosts = options["-H"]
    if "-p" in options:
        preserve = True
    api = "s3"  # TODO: support swift??

    # convert to seconds
    try:
        duration = int(duration) * 60
    except ValueError:
        print 'duration must be a number (minutes)'
        exit(1)

    if leftover and len(leftover) >= 2:
        action = leftover[0]
        bucket = leftover[1]
        if len(leftover) >= 3:
            mode = leftover[2]

    if not key or not secret or not bucket or not action:
        cli_help(sys.argv[0])
        exit(1)

    if not endpoint:
        try:
            endpoint = os.environ['BOURNE_DATA_IPADDR']
        except KeyError:
            print 'you must specify an endpoint with -e <endpoint> or in the BOURNE_DATA_IPADDR env var'
            exit(1)

    if not hosts:
        hosts = _get_peer_facing_ip(endpoint)

    fa = ViprFileAccess(api, endpoint, key, secret)

    try:
        if action == 'getmode':
            pprint.pprint(fa.get_bucket_mode(namespace, bucket))
        elif action == 'setmode':
            if not mode:
                print 'mode must be specified'
                exit(1)
            pprint.pprint(fa.set_bucket_mode(namespace, bucket, mode, hosts, duration, token, uid, preserve))
        elif action == 'getaccess':
            print_exports(fa.get_bucket_access(namespace, bucket))
        else:
            cli_help(sys.argv[0])
            exit(1)
    except Exception as e:
        print 'There was an error:'
        if "-v" in options:
            print traceback.format_exc()
        else:
            print e.message
        exit(2)
