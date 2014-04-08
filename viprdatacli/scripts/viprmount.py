import sys
import os
import getopt
import traceback
from viprdatacli.fileaccess import ViprMount, _get_peer_facing_ip, cli_version


def cli_help(script_name):
    print 'usage:'
    print '    ' + script_name + ' -b <bucket> -k <access_key> -s <secret_key> [..] [local_dir]'
    print 'other options:'
    print '    -h                 : print this help text'
    print '    -v                 : print traceback on errors'
    print '    -V                 : print the version of the main library'
    print '    -r                 : mount read-only'
    print '    -e <endpoint>      : specify the data node endpoint (required if'
    print '                         the BOURNE_DATA_IPADDR env var is not set)' 
    print '    -k <access_key>    : specify your vipr access_key (this key must'
    print '                         own the bucket)'
    print '    -s <secret_key>    : the secret key associated with the access key'
    print '    -n <namespace>     : specify the namespace of the tenant (optional)'
    print '    -b <bucket>        : the bucket containing the objects to export'
    print '    -t <token>         : specify the accessmode token; will only'
    print '                         mount objects newer than the token'
    print '    -d <minutes>       : specify the duration the mount will last'
    print '                         in minutes (defaults to 12 hours)'
    print '    -u <local_uid>     : specify the client uid which should have'
    print '                         access to the mounted files (defaults to the'
    print '                         uid running this script)'
    print '    -p                 : preserves the paths of objects that were'
    print '                         originally ingested from an NFS export'
    print '                         * requires ViPR 1.1+'
    print '    local_dir          : specify the local directory under which mount'
    print '                         points will be created (defaults to .)'
    print '                         unnecessary on Windows (mounts will be'
    print '                         drive letters)'
    print 'NOTE: specifying a client UID of 0 is not recommended and may fail.'
    print '      if mounting as root (sudo), specify a different UID with -u'


#main
def main():
    #----------------------------------------------------------------------
    # command-line parsing
    #----------------------------------------------------------------------
    endpoint = key = secret = namespace = bucket = token = readonly = preserve = None
    parent_dir = '.'
    duration = 60 * 12
    
    opts, leftover = getopt.getopt(sys.argv[1:], "hvVre:n:b:k:s:t:d:l:u:p")
    options = dict(opts)
    
    if "-h" in options:
        cli_help(sys.argv[0])
        exit(0)
    if "-V" in options:
        cli_version(sys.argv[0])
        exit(0)

    if "-r" in options:
        readonly = True
    if "-e" in options:
        endpoint = options["-e"]
    if "-n" in options:
        namespace = options["-n"]
    if "-b" in options:
        bucket = options["-b"]
    if "-k" in options:
        key = options["-k"]
    if "-s" in options:
        secret = options["-s"]
    if "-t" in options:
        token = options["-t"]
    if "-d" in options:
        duration = options["-d"]
    if os.name == "nt":
        uid = 1001  # Registry settings are needed to set the anon UID in Windows
    else:
        uid = os.getuid()
    if "-u" in options:
        uid = options["-u"]
    if "-p" in options:
        preserve = True
    api = "s3"  # TODO: support swift??
    if leftover:
        parent_dir = leftover[0]
    
    if not bucket or not key or not secret:
        cli_help(sys.argv[0])
        exit(1)
    
    if not endpoint:
        try:
            endpoint = os.environ['BOURNE_DATA_IPADDR']
        except KeyError:
            print 'you must specify an endpoint with -e <endpoint> or in the BOURNE_DATA_IPADDR env var'
            exit(1)
    
    hosts = _get_peer_facing_ip(endpoint)

    try:    
        vmount = ViprMount(api, endpoint, key, secret, namespace, bucket, token, hosts, readonly, uid, duration,
                           preserve, parent_dir)
        vmount.execute()
    except Exception as e:
        print 'There was an error:'
        if "-v" in options:
            print traceback.format_exc()
        else:
            print e.message
        exit(2)
