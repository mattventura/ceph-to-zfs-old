import argparse
import os.path
import sys

from ceph_to_zfs.jobcontrol import GlobalControl
from ceph_to_zfs.statuslogger import TopLevelLogger
from ceph_to_zfs.web import WebController

try:
    import rados
    import rbd
except ModuleNotFoundError:
    print('Using backup ceph libs - please install python3-ceph')
    import cephlibs.rados as rados
    import cephlibs.rbd as rbd


def run():
    parser = argparse.ArgumentParser(prog='ceph-to-zfs', description='Utility to back up Ceph to ZFS')

    parser.add_argument('-c', '--config', default='/etc/ceph-to-zfs/config.py', help='Path to config file',
                        metavar='/path/to/config.py')
    parser.add_argument('-d', '--daemon', action='store_true',
                        help='Run in daemon mode. If not specified, run jobs then exit. Runs in foreground.')
    # parser.add_argument('-b', '--background', action='store_true',
    #                     help='When using -d (daemonize), fork into background.')
    parser.add_argument('-w', '--web', action='store_true', help='Enable web API and interface')
    # TODO: flag to select which jobs to run
    args = parser.parse_args()
    cfg = args.config
    if not os.path.isfile(cfg):
        print(f'Config file {cfg} does not exist. '
              + 'Either place your configuration file at /etc/ceph-to-zfs/config.py, or use the -c option to specify '
              + 'its location.')
        sys.exit(50)

    if args.web and not args.daemon :
        print(f'-w/--web does not make sense without -d/--daemon')
        sys.exit(50)

    gbc = GlobalControl.from_config_file(cfg)
    if args.daemon:
        gbc.activate_loop()
        if args.web:
            web_logger = TopLevelLogger('Web Server')
            web = WebController(gbc, web_logger)
            web.start_web()
    else:
        gbc.run_all_jobs()
