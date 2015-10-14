#!/usr/bin/env python
# encoding: utf-8
#
# Authors:
#   Pierre Mavro <p.mavro@criteo.com> / <pierre@mavro.fr>
#
# Packages dependencies (Debian/Ubuntu):
#   libcurl4-gnutls-dev
#   python-krbV
#
# Python dependencies:
#   krbcontext
#   requests-kerberos

# Todo: aller chercher les fichiers dans les snapshots avant de les push !!!
# Todo: vérifier qu'il n'y a plus de ligne du type:  [ERROR] Can't read system-sstable_activity-jb-4473-Index.db, check if file exists and permissions
# Todo: les gets table doivent foirer du fait de la dernière modif sur les paths
# Todo: lorsqu'un fichier existe déjà, faire un test de checksum avant d'override
# Todo: ajouter le clear des snapshots
# Todo: ajouter l'exclusion des

__version__ = '0.1'

import argparse
import sys
import os
import ConfigParser
import datetime
import logging
import requests
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import socket
import random
import urllib3
import re
import subprocess
import json
import yaml
import socket

LVL = {'INFO': logging.INFO,
       'DEBUG': logging.DEBUG,
       'ERROR': logging.ERROR,
       'CRITICAL': logging.CRITICAL}


def setup_log(name=__name__, level='INFO', log=None, console=True, form='%(asctime)s [%(levelname)s] %(message)s'):
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


def create_connection_replacement(address, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, source_address=None,
                                  socket_options=None):
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
      idx = random.randint(0, len(addrinfo) - 1)
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
   def __init__(self, username, realm, kerberos, keytab, cassandra_data_path, cassandra_config, hadoop_url,
                hadoop_dest_dir, dry_run, logger=__name__):
      """
      :type username: str
      :type realm: str
      :type dry_run: bool
      :type keytab: str
      :type cassandra_data_path: str
      :type cassandra_config: str
      :type kerberos: bool
      :type hadoop_dest_dir: str
      :type hadoop_url: str
      :type logger: str
      """
      self.username = username
      self.realm = realm
      self.kerberos = kerberos
      self.keytab = keytab
      self.cassandra_data_path = cassandra_data_path
      self.cassandra_config = cassandra_config
      self.hadoop_url = hadoop_url
      self.hadoop_dest_dir = hadoop_dest_dir
      self.dry_run = dry_run
      self.hostname = socket.gethostname()

      self.meta_dir = 'cass_snap_metadata'
      self.logger = logging.getLogger(logger)
      self.cluster_name = self._get_cluster_name()
      self.session = requests.Session()
      self.auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)

      self.check_requirements()
      self.connect_to_hadoop()

   def check_requirements(self):
      """
      Checking requirements for the overall usage
      """
      # Check keytab permissions
      if self.keytab is not None:
         if os.path.isfile(self.keytab):
            if not os.access(self.keytab, os.R_OK):
               self.logger.critical("Do not have permission to read keytab file" % self.keytab)
               sys.exit(1)
         self.logger.debug("Keytab file is readable (%s)" % self.keytab)

      # Check cassandra data path permissions
      try:
         if not os.access(self.cassandra_data_path, os.R_OK):
            self.logger.critical("Can't have permissions to read (%s) Cassandra data folder" % self.cassandra_data_path)
            sys.exit(1)
         else:
            self.logger.debug(
               'Cassandra data path permission (%s): ok' % self.cassandra_data_path)
      except Exception as e:
         self.logger.debug('Cassandra data path permission (%s) failed: %s' % (self.cassandra_data_path, e))

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
         # Try to connect 3 times to Hadoop
         try_con = 0
         while try_con < 3:

            try:
               self.logger.debug('Trying to authenticate to Hadoop')

               r = self.session.get('/'.join([self.hadoop_url, '?op=GETHOMEDIRECTORY']), auth=self.auth)
               if r.status_code != 200:
                  self.logger.error("Can't get Hadoop connexion : %s" % str(r.status_code))
                  self.session.close()
                  try_con += 1
               else:
                  self.logger.debug('Connexion to Hadoop: successful')
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
         url = ''.join([self.hadoop_url, '/', self.hadoop_dest_dir, '?op=liststatus'])
         self.logger.debug(''.join(['used url: ', url]))

         r = self.session.get(url, auth=self.auth)
         if r.status_code != 200:
            raise Exception("Failed listing Hadoop directory: " + str(r.status_code))

      except IndexError, e:
         self.logger.critical("Can't connect to Kerberos : %s" % e)
         sys.exit(1)

   def _get_keyspaces_list(self):
      """
      Get the keyspaces and table list

      :rtype list
      """
      self.logger.debug('Getting cassandra keyspaces and tables lists')
      ks_list = os.listdir(self.cassandra_data_path)
      # self.logger.debug("Keyspaces: %s" % str(ks_list))
      return ks_list

   def _get_tables_list(self, ks_list):
      """
      Get all the tables list from the list of keyspaces in argument

      :type ks_list: list
      :rtype list
      """
      tables_list = []

      for table in ks_list:
         ks_tables = os.listdir('/'.join([self.cassandra_data_path, table]))
         tables_list += ['/'.join([table, file]) for file in ks_tables]
         # self.logger.debug("Tables: %s" % str(tables_list))

      return tables_list

   def _get_current_snapshot_files(self, snap_name, tables_list):
      """
      Get the list of current tables in a snapshot folder

      :type snap_name: str
      :type tables_list: list
      :rtype: list
      """
      current_snapshot = []

      try:

         for table in tables_list:
            snap_path = '/'.join([self.cassandra_data_path, table, 'snapshots', snap_name])
            if not os.path.isdir(snap_path):
               continue

            for i in os.listdir(snap_path):
               current_snapshot.append('/'.join([table, i]))
            current_snapshot.sort()

      except Exception as e:
         self.logger.critical("Could not list tables in cassandra data dir")

      return current_snapshot

   def _create_snapshot_file(self, snap_name, tables_list):
      """
      Create a snapshot list with list of files which will be stored on the
      Hadoop Cluster. You need to pass snapshot name number in argument. This
      will return a list of files made by the snapshot.

      :type snap_name: str
      :type tables_list: list
      :rtype: list, str
      """
      today = datetime.datetime.now().strftime('%Y_%m_%d')
      snap_file = ''.join(['/tmp/', 'cass_snap_', today])
      current_snap = self._get_current_snapshot_files(snap_name, tables_list)

      try:
         self.logger.debug("Storing file information in %s" % snap_file)
         with open(snap_file, 'a') as f:
            for table in current_snap:
               f.write(re.sub(r"/\n$", "\n" ,'/'.join([table, "\n"])))
      except Exception as e:
         self.logger.critical("Could not write tables list to file: %s" % e)

      return current_snap, snap_file

   def _get_last_snapshot_file(self):
      """
      Get the last snapshot_file on Hadoop

      :rtype: str
      """
      self.logger.debug('Listing metadata directory from Hadoop')
      url = ''.join([self.hadoop_url, self.hadoop_dest_dir, '/', self.meta_dir, '/full?op=LISTSTATUS'])
      self.logger.debug(''.join(['used url: ', url]))

      r = self.session.get(url, auth=self.auth)

      if r.status_code == 404:
         self.logger.info("Cannot get meta file, folder does not exist : %s" % str(r.status_code))
         #self.session.close()
         return None

      # Deserialize json and get the latest snapshot meta file
      snaps_json = json.loads(r._content)
      all_snaps = {}
      for s in snaps_json['FileStatuses']['FileStatus']:
         all_snaps[s['pathSuffix']] = s['modificationTime']
      self.logger.debug("Found %d snapshot(s) on Hadoop" % len(all_snaps))

      # Check if empty
      if len(all_snaps) == 0:
         self.logger.info("No metadata files were found")
         return None

      latest = max(all_snaps.iterkeys(), key=(lambda key: all_snaps[key]))
      self.logger.debug("Latest snapshot on Hadoop is: %s" % latest)

      return latest

   def _get_cluster_name(self):
      """
      Get the cluster name from cassandra configuration file
      :return: str
      """
      self.logger.debug("Getting cluster name in cassandra config file: %s" % self.cassandra_config)

      try:
         stream = open(self.cassandra_config, 'r')
         config = yaml.load_all(stream)

         for line in config:
            for k,v in line.items():
               if k == 'cluster_name':
                  self.logger.debug("Cluster name found: %s" % v)
                  return v

      except IndexError, e:
         self.logger.debug('Cluster name not found, using default one instead')

      return 'cassandra_cluster'

   def _hadoop_create_folders(self, folders):
      """
      Create the required folders (for metadata) to prepare the dump

      :type folders: list
      """

      for folder in folders:

         try:
            # Todo: rajouter une option pour les permissions
            url = ''.join([self.hadoop_url, self.hadoop_dest_dir, '/', folder, '?op=MKDIRS'])

            r = self.session.put(url, auth=self.auth)
            #self.logger.debug(''.join(['url: ', url, ' / ', str(r.status_code)]))

            if r.status_code == 500:
               action = self.session.put(r.url, auth=self.auth)
               #self.logger.debug(''.join(['Fwd mkdir: ', r.url, ' / ', str(action.status_code)]))

               if action.status_code != 200:
                  raise Exception('Failed to create ' + folder + ' directory: ' + str(action.status_code))
                  # Todo: ne pas sortir comme un sauvage, faire une fonction pour exit
                  sys.exit(1)

         except IndexError, e:
            self.logger.error(" %s" % e)
            sys.exit(1)

   def _push_file_to_hadoop(self, file_path, dst_path=''):
      """
      Push files to Hadoop
      You need to set the file name (with path) from the source path to the destination path

      :type file_path: str
      :type dst_path: str

      """
      # Required header to upload
      headers = {'content-type': 'application/octet-stream'}

      # Get source file and path
      file = os.path.basename(file_path)
      src_path = os.path.dirname(file_path)
      #self.logger.debug("Source path: %s / file: %s / dest: %s" % (src_path, file, dst_path))
      self.logger.debug("Uploading: %s/%s" % (src_path, file))

      # Check permissions
      os.chdir(src_path)
      if not os.access(file, os.R_OK):
         self.logger.error("Can't read %s, check if file exists and permissions" % file)
         return False

      # Build URL
      url = ''.join([self.hadoop_url, self.hadoop_dest_dir, '/', dst_path, '/', file, '?op=CREATE&overwrite=true'])

      try_con = 0
      while try_con < 3:

         try:

            if try_con == 4:
               return False

            r = self.session.put(url, auth=self.auth)
            #self.logger.debug("Pushing: %s" % url)

            if r.status_code == 500:
               action = self.session.put(r.url, data=file, auth=self.auth, headers=headers)

               if action.status_code == 201:
                  return True

               self.logger.error("Failed to push table %s: %s" % (url, str(action.status_code)))
               try_con += 1
            else:
               try_con += 1

         except IndexError, e:
            self.logger.error("Could not upload %s: %s" % (table, e))

   def _push_tables_to_hadoop(self, tables_list):
      """
      Push tables in the list to Hadoop cluster. This will use the cluster name
      as well and create a dedicated folder for it, just in case you're using
      the same Hadoop account for several cassandra clusters.

      :type tables_list: list
      """

      # Create mandatory folders to manage snapshots
      self.logger.debug('Creating mandatory folders in hadoop if do not exist')
      self._hadoop_create_folders([self.cluster_name, self.meta_dir])

      # Create Cassandra folders from Cassandra snapshot tables list
      self.logger.debug('Creating cassandra snapshot folder in hadoop if do not exist')
      folders = ['/'.join([self.cluster_name, os.path.dirname(table)]) for table in tables_list]
      self._hadoop_create_folders(list(set(folders)))

      # Push sstables to Hadoop
      self.logger.info('Pushing snapshot tables to hadoop, please wait...')
      failed_tables = [table for table in tables_list
                       if not self._push_file_to_hadoop('/'.join([self.cassandra_data_path, table]),
                                                        '/'.join([self.cluster_name, os.path.dirname(table)]))]
      self.logger.debug("There are %d tables which could not be uploaded" % len(failed_tables))

   def make_snapshot(self):
      """
      Performing Cassandra snapshot and pushing it to Hadoop
      """
      # Get local keyspaces and tables list
      ks_list = self._get_keyspaces_list()
      tables_list = self._get_tables_list(ks_list)

      # Locally snapshot all keyspaces
      try:
         self.logger.info('Start snapshoting')
         result = subprocess.Popen('nodetool snapshot', shell=True, stdout=subprocess.PIPE)
      except IndexError, e:
         self.logger.critical("Error during snapshot request : %s" % e)
         sys.exit(1)

      # Get snapshot name from nodetool result
      for line in result.stdout:
         if re.match('Snapshot directory:', line):
            snap_name = re.match(r"Snapshot directory: (\d+)", line).group(1)
            self.logger.debug("Snapshot name: %s" % snap_name)
      try:
         snap_name
      except NameError:
         self.logger.critical("Could not find snapshot name")
         sys.exit(1)

      # Generate a diff between last and current snap
      last_snapshot = self._get_last_snapshot_file()
      current_snap, snap_file = self._create_snapshot_file(snap_name, tables_list)

      if last_snapshot is None:
         tables_to_upload = current_snap
      else:
         tables_to_upload = set(current_snap) - set(last_snapshot)
      self.logger.debug("Tables changes before last snapshot: %d" % len( tables_to_upload))

      # Send diff tables to hadoop
      self._push_tables_to_hadoop(tables_to_upload)

      # Push metadata to hadoop
      self.logger.info('Pushing metadata to hadoop')
      self._hadoop_create_folders(['/'.join([self.meta_dir, self.cluster_name, self.hostname])])
      self._push_file_to_hadoop(snap_file, '/'.join([self.meta_dir, self.cluster_name, self.hostname]))

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
   parser.add_argument('-u', '--username', action='store', type=str, default=None, metavar='USERNAME',
                       help='Kerberos username / principal')
   # parser.add_argument('-p', '--password', action='store',
   #               type=str, default=None, metavar='PASSWORD',
   #               help='Password if no keytab is not used')
   parser.add_argument('-r', '--realm', action='store', type=str, default=None, metavar='REALM', help='Kerberos Realm')
   parser.add_argument('-k', '--kerberos', action='store_false', default=False, help='Request kerberos authentication')
   parser.add_argument('-t', '--keytab', action='store', type=str, default=None, metavar='KEYTAB',
                       help='Keytab file path')

   # Cassandra
   parser.add_argument('-p', '--cassandra_data_path', action='store', default='/var/lib/cassandra/data',
                       metavar='CASSANDRA_DATA_PATH', help='Path to Cassandra data directory')
   parser.add_argument('-n', '--cassandra_config', action='store', default='/etc/cassandra/conf/cassandra.yaml',
                       metavar='CASSANDRA_CONFIG', help='Path to Cassandra configuration file')
   # Todo: ignorer certaines tables (opscenter)

   # Hadoop
   parser.add_argument('-o', '--hadoop_url', action='store', type=str, default=None, metavar='HADOOP_URL',
                       help='HADOOP_URL')
   parser.add_argument('-e', '--hadoop_dest_dir', action='store', type=str, default=None, metavar='HADOOP_DEST_DIR',
                       help='HADOOP_DEST_DIR')

   # Config
   parser.add_argument('-c', '--configuration_file', action='store', type=str,
                       default=''.join( [os.path.expanduser("~"), '/.cs2h.conf']), metavar='CREDENTIALS',
                       help='Credentials file path')

   # Actions
   parser.add_argument('-L', '--list_snaps', action='store_true', default=False, help='List available snapshots')
   parser.add_argument('-S', '--make_snapshot', action='store_true', default=False,
                       help='Make a snapshot and store it on Hadoop')
   parser.add_argument('-R', '--restore_snapshot', action='store_true', default=False,
                       help='Restore a snapshot from Hadoop from a date')
   parser.add_argument('-C', '--clear_snapshot', action='store_false', default=False,
                       help='Clear snapshot. If launched with -S option, it will be done after the snapshot transfer'
                            'to Hadoop')
   parser.add_argument('-D', '--dry_run', action='store_false', default=True,
                       help='Define if it should make snapshot or just dry run')

   # Logs and debug
   parser.add_argument('-f', '--file_output', metavar='FILE', default=None, action='store', type=str,
                       help='Set an output file')
   parser.add_argument('-s', '--stdout', action='store_true', default=True, help='Log output to console (stdout)')
   parser.add_argument('-v', '--verbosity', metavar='LEVEL', default='INFO', type=str, action='store',
                       help='Verbosity level: DEBUG/INFO/ERROR/CRITICAL')

   parser.add_argument('-V', '--version', action='version', version=' '.join([__version__, 'Licence GPLv2+']),
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

         if arg.cassandra_data_path == parser.get_default(
                 'cassandra_data_path'):
            cass_dpath = args_validation('cassandra_data_path')
            if cass_dpath is not None:
               arg.cassandra_data_path = cass_dpath
         if arg.cassandra_config == parser.get_default('cassandra_config'):
            # Todo: l'override marche pas
            cass_config = args_validation('cassandra_config')
            if not cass_config:
               arg.cassandra_config == cass_config

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
                              arg.cassandra_data_path, arg.cassandra_config,
                              arg.hadoop_url, arg.hadoop_dest_dir,
                              arg.dry_run)
   if arg.list_snaps:
      operation.list_snapshots()
   if arg.make_snapshot:
      operation.make_snapshot()


if __name__ == "__main__":
   main()
