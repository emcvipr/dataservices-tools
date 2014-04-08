import sys
import getopt
import traceback
from viprdatacli.fileaccess import ViprUmount, cli_version


def cli_help(script_name):
    print 'usage: ' + script_name + ' [-h] [local_dir]'
    print 'options:'
    print '    -h                 : print this help text'
    print '    -v                 : print traceback on errors'
    print '    -V                 : print the version of the main library'
    print '    local_dir          : the local directory under which mount points'
    print '                         were created (defaults to .)'


# main
def main():
    #----------------------------------------------------------------------
    # command-line parsing
    #----------------------------------------------------------------------
    opts, leftover = getopt.getopt(sys.argv[1:], "hvV")
    options = dict(opts)

    if "-h" in options:
        cli_help(sys.argv[0])
        exit(0)
    if "-V" in options:
        cli_version(sys.argv[0])
        exit(0)

    parent_dir = '.'
    if len(leftover) > 0:
        parent_dir = leftover[0]

    try:
        vumount = ViprUmount(parent_dir)
        vumount.execute()
    except Exception as e:
        print 'There was an error:'
        if "-v" in options:
            print traceback.format_exc()
        else:
            print e.message
        exit(2)
