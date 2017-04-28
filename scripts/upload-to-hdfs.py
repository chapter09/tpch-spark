#!/usr/bin/python3

import os
import sys
import subprocess
from pyhocon import ConfigFactory

RAW_DATA = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "partsupp",
    "part",
    "region",
    "supplier"]


def run(cmd):
    return subprocess.run(cmd, shell=True, cwd=os.environ["HADOOP_HOME"])


def put(f, dst, opts):
    cmd = "./bin/hdfs dfs -put %s %s" % (f, dst)


def mkdir(p):
    cmd = "./bin/hdfs dfs -mkdir %s" % p


def parse_config():
    return ConfigFactory.parse_file('../conf/application.conf')


def raw_data_list(data_scale):
    dbgen_path = os.path.abspath("../dbgen/")
    return [os.path.join(dbgen_path, d + "-" + str(data_scale) + ".tbl") for d in RAW_DATA]


def main():
    if not os.path.exists(os.environ["HADOOP_HOME"]):
        print("Please Specify HADOOP_HOME")
        exit(0)

    conf = parse_config()
    procs = []
    data_scale =  conf.get_int("all.data-scale")
    input_dir = conf.get_string("all.input-dir")

    data_file_list = raw_data_list(data_scale)

    for f in data_file_list:
        pass






if __name__ == '__main__':
    main()
