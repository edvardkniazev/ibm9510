#!/usr/bin/python3

from datetime import datetime
import time
import argparse
import json
from clickhouse_driver import Client
import xml.dom.minidom
import glob
import os
from paramiko import SSHClient, AutoAddPolicy
from scp import SCPClient, SCPException
from pprint import pprint

import sys

def get_arguments():
    hostname = None
    ipaddress = None

    parser = argparse.ArgumentParser(
        description='This script asks data from IBM 9510 and inserts it to database')
    parser.add_argument(
        '--hostname',
        required=True,
        type=str,
        help='the hostname of the IBM 9510 as we want to see in the database')
    parser.add_argument(
        '--ipaddress',
        required=True,
        type=str,
        help='the IP-address of the IBM 9510 to ssh-connect')

    args = parser.parse_args()
    return args.hostname, args.ipaddress


def str_to_unixtime(strdate):
    date = datetime.strptime(strdate, '%Y-%m-%d %H:%M:%S')
    return int(date.timestamp())


def parse_data(file, hostname):
    version = int(time.time())
    data = []

    nvstat = xml.dom.minidom.parse(file)
    head = nvstat.firstChild
    strdate = head.getAttribute('timestamp')
    timestamp = str_to_unixtime(strdate)
    vdsks = nvstat.getElementsByTagName('vdsk')

    for vd in vdsks:

        # values:
        #     metric, hostname, volumename, timestamp, value, version
        for metric in ('ro', 'wo', 'rb', 'wb', 'rl', 'wl'):
            values = (metric, hostname, vd.getAttribute('id'),
                  timestamp, float(vd.getAttribute(metric)), version)
            data.append(values)
    return data


def create_table(clickhouse, tablename):
    sql = ''' CREATE TEMPORARY TABLE IF NOT EXISTS {tablename}(
                  metric String,
                  hostname String,
                  volumename String,
                  timestamp UInt32,
                  value Float64,
                  version UInt32)
              ENGINE = Memory
          '''.format(tablename=tablename)
    clickhouse.execute(sql)


def opendb():
    """
    Data for connecting to the database are contained in the json-file:
{
    "clickhouse":{
        "host":"192.168.xxx.xxx",
        "database":"xxxxxx",
        "user":"xxxxxx",
        "password":"xxxxxxxx"
    }
}

    """
    with open('ibm9510.json') as data:
        config = json.load(data)

    database = config['clickhouse']['database']
    user = config['clickhouse']['user']
    password = config['clickhouse']['password']
    host = config['clickhouse']['host']

    clickhouse = Client(
        database=database,
        user=user,
        password=password,
        host=host)
    return clickhouse


def closedb(clickhouse):
    clickhouse.disconnect()


def insert_data(clickhouse, tablename, data):
    sql = ''' INSERT INTO {tablename}(
                  metric, hostname, volumename, timestamp, value, version)
              VALUES
          '''.format(tablename=tablename)
    clickhouse.execute(sql, data)


def update_data(clickhouse, tablename):
    sql = ''' INSERT INTO {tablename}
              SELECT
                  metric,
                  hostname,
                  volumename,
                  0 timestamp,
                  min(value) value,
                  max(version) version
              FROM ibm9150c2
              GROUP BY
                  metric,
                  hostname,
                  volumename,
                  timestamp
            '''.format(tablename=tablename)
    clickhouse.execute(sql)

    sql = ''' INSERT INTO ibm9150d2
              SELECT
                  metric,
                  hostname,
                  volumename,
                  timestamp,
                  delta,
                  version
              FROM
              (
                  SELECT
                      metric,
                      hostname,
                      volumename,
                      timestamp,
                      runningDifference(value) / runningDifference(timestamp) AS delta,
                      version
                  FROM
                  (
                      SELECT
                          metric,
                          hostname,
                          volumename,
                          timestamp,
                          value,
                          max(version) version
                      FROM {tablename}
                      GROUP BY
                          metric,
                          hostname,
                          volumename,
                          timestamp,
                          value
                      ORDER BY
                          metric ASC,
                          hostname ASC,
                          volumename ASC,
                          timestamp ASC
                   )
              )
              GROUP BY
                  metric,
                  hostname,
                  volumename,
                  timestamp,
                  delta,
                  version
              HAVING
                  timestamp > 0
          '''.format(tablename=tablename)
    clickhouse.execute(sql)

    sql = ''' OPTIMIZE TABLE ibm9150d2

          '''
    clickhouse.execute(sql)


def get_to_dir():
    """
    Get unique directory name
    """
    to_dir = './tmp' + str(os.getpid())
    return to_dir


def cp_files(ipaddress, to_dir):
    """
    Get statistics files
    """
    username = 'monitor'
    sshkey = '/home/zabbix/.ssh/id_rsa'
    path = '/dumps/iostats/'
    fn_reg = 'Nv_stats_*'
    ssh = SSHClient()
    ssh.load_system_host_keys(sshkey)
    ssh.set_missing_host_key_policy(AutoAddPolicy())
    ssh.connect(hostname=ipaddress, username=username, key_filename=sshkey)

    with SCPClient(ssh.get_transport()) as scp_client:
        try:
            scp_client.get(remote_path=path, recursive=True, local_path=to_dir)
        except SCPException:
            pass


def get_list_files(path, word):
    files = []
    for f in os.listdir(path):
        pf = os.path.join(path, f)
        if word in f and os.path.isfile(pf):
            files.append(pf)
    return files


def rm_files(path):
    """
    Remove copied files
    """
    files = get_list_files(path=path, word='')
    for file in files:
        os.remove(file)
    try:
        os.rmdir(path)
    except:
        pass

def main():
    hostname, ipaddress = get_arguments()
    clickhouse = opendb()

    tablename = hostname.split('-')[-1]
    create_table(clickhouse, tablename)

    to_dir = get_to_dir()
    cp_files(ipaddress, to_dir)
    files = get_list_files(path=to_dir, word='Nv_stats_')
    for file in files:
        data = parse_data(file, hostname)
        insert_data(clickhouse, tablename, data)
    update_data(clickhouse, tablename)

    rm_files(to_dir)
    closedb(clickhouse)

if __name__ == "__main__":
    main()

