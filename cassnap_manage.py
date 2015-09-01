#!/usr/bin/env python
# encoding: utf-8
#
# Authors:
#   Pierre Mavro <p.mavro@criteo.com> <pierre@mavro.fr>
#
# Packages dependencies (Debian/Ubuntu):
#   libcurl4-gnutls-dev
#   python-krbV
#
# Python dependencies:
#   krbcontext
#   requests-kerberos
#

__version__ = 'v0.1'

import argparse
import sys
import os
import ConfigParser
import time
import datetime
import logging
import requests
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import socket
import random
import urllib3

LVL = {'INFO': logging.INFO,
       'DEBUG': logging.DEBUG,
       'ERROR': logging.ERROR,
       'CRITICAL': logging.CRITICAL}

def setup_log(name=__name__, level='INFO', log=None,
              console=True, form='%(asctime)s [%(levelname)s] %(message)s'):
    """
    Setup logger object for displaying information into console/file

    :param name: Name of the logger object to create
    :type name: str

    :param level: Level INFO/DEBUG/ERROR etc
    :type level: str

    :param log: File to which log information
    :type log: str

    :param console: If log information sent to console as well
    :type console: Boolean

    :param form: The format in which the log will be displayed
    :type form: str

    :returns: The object logger
    :rtype: logger object
    """
    level = level.upper()
    if level not in LVL:
        logging.warning("Option of log level %s incorrect, using INFO." % level)
        level = 'INFO'
    level = LVL[level]
    formatter = logging.Formatter(form)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if log is not None:
        filehdl = logging.FileHandler(log)
        filehdl.setFormatter(formatter)
        logger.addHandler(filehdl)
    if console is True:
        consolehdl = logging.StreamHandler()
        consolehdl.setFormatter(formatter)
        logger.addHandler(consolehdl)
    return logger

def create_connection_replacement(address,
                                  timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
                                  source_address=None, socket_options=None):
    """
    Overriding urllib3 to avoid possible 404 issue with Hadoop gateways

    Sometimes, during a connexion to an hadoop cluster, you may not be able to
    reach an hadoop web gateway (404 return). As 404 is not considered as
    failed, urllib3 try to switch to another server in a round robin DNS case.
    This override will permit to randomize the connexion IP when initializing
    the first connexion to hadoop to avoid a such issue.
    """
    host, port = address
    if host.startswith('['):
        host = host.strip('[]')
    err = None

    addrinfo = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    while len(addrinfo) > 0:
        idx = random.randint(0, len(addrinfo)-1)
        af, socktype, proto, canonname, sa = addrinfo[idx]
        del addrinfo[idx]

        sock = None
        try:
            sock = socket.socket(af, socktype, proto)

            # If provided, set socket level options before connecting.
            # This is the only addition urllib3 makes to this function.
            urllib3.util.connection._set_socket_options(sock, socket_options)

            if timeout is not socket._GLOBAL_DEFAULT_TIMEOUT:
                sock.settimeout(timeout)
            if source_address:
                sock.bind(source_address)
            sock.connect(sa)
            return sock

        except socket.error as _:
            err = _
            if sock is not None:
                sock.close()
                sock = None

    if err is not None:
        raise err
    else:
        raise socket.error("getaddrinfo returns an empty list")

class ManageSnapshot:

    def __init__(self, username, realm, kerberos, keytab,
                 hadoop_url, hadoop_dest_dir, dry_run, logger=__name__):
        """

        :type username: str
        :type realm: str
        :type logger: str
        :type dry_run: bool
        :type keytab: str
        :type kerberos: bool
        :type hadoop_dest_dir: str
        :type hadoop_url: str
        """
        self.username = username
        self.realm = realm
        self.kerberos = kerberos
        self.keytab = keytab
        self.hadoop_url = hadoop_url
        self.hadoop_dest_dir = hadoop_dest_dir
        self.dry_run = dry_run

        self.logger = logging.getLogger(logger)
        self.session = requests.Session()
        self.auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        self.connect_to_hadoop()

    def connect_to_hadoop(self):
        """
        Connect to Hadoop and validate authentication
        """
        self.logger.info("Checking connexion to Hadoop")
        # Authenticate to kerberos if requested
        if self.kerberos is True:
            self.connect_hadoop_kerberos()

    def connect_hadoop_kerberos(self):
        """
        Connect to Hadoop with Kerberos Keytab
        """

        # Connect with Keytab
        if self.keytab is not None:
            if os.path.isfile(self.keytab):
                if not os.access(self.keytab, os.R_OK):
                    self.logger.critical("Do not have permission to read keytab"
                                         "file" % self.keytab)
                    sys.exit(1)
            self.logger.debug("Keytab file is readable (%s)" % self.keytab)

            # Try to connect 3 times to Hadoop
            try_con = 0
            while try_con < 3:
                try:
                    self.logger.debug('Trying to authenticate to Hadoop')
                    r = self.session.get('/'.join([self.hadoop_url,
                                                   '?op=GETHOMEDIRECTORY']),
                                         auth=self.auth)
                    if r.status_code != 200:
                        self.logger.critical("Can't get Hadoop connexion : %s" %
                                             str(r.status_code))
                        self.session.close()
                        try_con += 1
                    else:
                        try_con = 4
                except IndexError, e:
                    self.logger.critical("Can't connect to Kerberos : %s" % e)
                    sys.exit(1)
            if try_con != 4:
                sys.exit(1)
        else:
            print('Could not connect without Kerberos keytab to Hadoop Cluster')
            sys.exit(1)

    def list_snapshots(self):
        """
        List available snapshots from Hadoop
        """
        self.logger.info("Listing available Cassandra snapshots")
        try:
            url = ''.join([self.hadoop_url, '/',
                           self.hadoop_dest_dir, '?op=liststatus'])
            self.logger.debug(''.join(['used url: ', url]))
            r = self.session.get(url, auth=self.auth)
            if r.status_code != 200:
                raise Exception("Failed listing Hadoop directory: " +
                                str(r.status_code))
        except IndexError, e:
            self.logger.critical("Can't connect to Kerberos : %s" % e)
            sys.exit(1)

    def make_snapshot(self):
        """
        Performing Cassandra snapshot and pushing it to Hadoop
        """


def main():
    """
    Main - manage args
    """

    def args_validation(arg, arg_type='str'):
        try:
            if arg_type == 'str':
                return config.get('defaults', arg)
            elif arg_type == 'bool':
                return config.getboolean('defaults', arg)
        except:
            return None

    # Main informations
    parser = argparse.ArgumentParser(
        description='Cassandra snapshot to Hadoop utility',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Authentication
    parser.add_argument('-u', '--username', action='store', type=str,
                        default=None, metavar='USERNAME',
                        help='Kerberos username / principal')
    #parser.add_argument('-p', '--password', action='store',
    #                    type=str, default=None, metavar='PASSWORD',
    #                    help='Password if no keytab is not used')
    parser.add_argument('-r', '--realm', action='store', type=str, default=None,
                        metavar='REALM', help='Kerberos Realm')
    parser.add_argument('-k', '--kerberos', action='store_false', default=False,
                        help='Request kerberos authentication')
    parser.add_argument('-t', '--keytab', action='store', type=str,
                        default=None, metavar='KEYTAB', help='Keytab file path')

    # Hadoop
    parser.add_argument('-o', '--hadoop_url', action='store', type=str,
                        default=None, metavar='HADOOP_URL', help='HADOOP_URL')
    parser.add_argument('-e', '--hadoop_dest_dir', action='store', type=str,
                        default=None, metavar='HADOOP_DEST_DIR',
                        help='HADOOP_DEST_DIR')

    # Config
    parser.add_argument('-c', '--configuration_file', action='store', type=str,
                        default=''.join([os.path.expanduser("~"), '/.cs2h.conf']),
                        metavar='CREDENTIALS', help='Credentials file path')

    # Actions
    parser.add_argument('-L', '--list_snaps', action='store_true',
                        default=False, help='List available snapshots')
    parser.add_argument('-S', '--make_snapshot', action='store',
                        help='Restore a snapshot from Hadoop from a date')
    parser.add_argument('-R', '--restore_snapshot', action='store_true',
                        default=False,
                        help='Make a snapshot and store it on Hadoop')
    parser.add_argument('-D', '--dry_run', action='store_false', default=True,
                        help='Define if it should make snapshot or just dry run')

    # Logs and debug
    parser.add_argument('-f', '--file_output', metavar='FILE',
                        default=None, action='store', type=str,
                        help='Set an output file')
    parser.add_argument('-s', '--stdout', action='store_true', default=True,
                        help='Log output to console (stdout)')
    parser.add_argument('-v', '--verbosity', metavar='LEVEL', default='INFO',
                        type=str, action='store',
                        help='Verbosity level: DEBUG/INFO/ERROR/CRITICAL')

    parser.add_argument('-V', '--version',
                        action='version',
                        version=' '.join([__version__, 'Licence GPLv2+']),
                        help='Print version number')

    # Print help if no args supplied
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    arg = parser.parse_args()

    # Setup loger
    setup_log(console=arg.stdout, log=arg.file_output, level=arg.verbosity)

    # Read credential file and override by command args
    if os.path.isfile(arg.configuration_file):
        if os.access(arg.configuration_file, os.R_OK):
            config = ConfigParser.ConfigParser()
            config.read([str(arg.configuration_file)])
            if not arg.kerberos:
                arg.kerberos = args_validation('kerberos', 'bool')
            if arg.keytab is None:
                arg.keytab = args_validation('keytab')
            if arg.hadoop_url is None:
                arg.hadoop_url = args_validation('hadoop_url')
            if arg.hadoop_dest_dir is None:
                arg.hadoop_dest_dir = args_validation('hadoop_dest_dir')
            if arg.username is None:
                arg.username = args_validation('username')
            if arg.realm is None:
                arg.realm = args_validation('realm')
        else:
            print("You don't have permission to read configuration file")
            sys.exit(1)

    # Exit if hadoop information is empty
    if arg.hadoop_dest_dir is None or arg.hadoop_url is None:
        print('Please enter hadoop information')
        sys.exit(1)

    urllib3.util.connection.create_connection = create_connection_replacement

    # Create action
    operation = ManageSnapshot(arg.username, arg.realm,
                               arg.kerberos, arg.keytab,
                               arg.hadoop_url, arg.hadoop_dest_dir,
                               arg.dry_run)
    if arg.list_snaps:
        operation.list_snapshots()

if __name__ == "__main__":
    main()