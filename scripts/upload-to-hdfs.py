#!/usr/bin/python3

import os
import os.path as path
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
    return subprocess.Popen(cmd, shell=True, cwd=os.environ["HADOOP_HOME"])


def put(f, dst, opts=None):
    print("Upload %s to %s"% (f, dst))
    return run("./bin/hdfs dfs -put %s %s" % (f, dst))


def mkdir(p):
    print("Creating HDFS directory: " + p)
    return run("./bin/hdfs dfs -mkdir -p %s" % p)


def parse_config():
    print("Parsing config file")
    return ConfigFactory.parse_file('../conf/application.conf')


def main():
    if not path.exists(os.environ["HADOOP_HOME"]):
        print("Please Specify HADOOP_HOME")
        exit(0)

    conf = parse_config()
    data_scale = conf.get_int("all.data-scale")
    input_dir = conf.get_string("all.input-dir")
    dbgen_path = path.abspath("../dbgen/")

    procs = []
    for d in RAW_DATA:
        procs.append(mkdir(input_dir + "/" + d + "-" + str(data_scale)))

    [p.wait() for p in procs]

    procs = []

    for d in RAW_DATA:
        procs.append(put(path.join(
            dbgen_path, d + "-" + str(data_scale) + ".tbl"),
            input_dir + "/" + d + "-" + data_scale + ".txt"))

    [p.wait() for p in procs]


if __name__ == '__main__':
    main()
