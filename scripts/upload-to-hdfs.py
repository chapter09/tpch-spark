#!/usr/bin/python3

import os
import os.path as path
import subprocess
from pyhocon import ConfigFactory
import argparse

RAW_DATA = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "partsupp",
    "part",
    "region",
    "supplier"]

DST = {'hao-ml-1': ['lineitem', 'supplier'],
       'hoa-ml-7': ['orders', 'part'],
       'hoa-ml-6': ['partsupp', 'region'],
       'hao-ml-8': ['customer', 'nation']}

INIT_SF = 15
SF = 15

def run(cmd):
    return subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, 
                            stderr=subprocess.STDOUT,
                            cwd=os.environ["HADOOP_HOME"])


def put(f, dst):
    print("Upload %s to %s" % (f, dst))
    return run("./bin/hdfs dfs -put -f %s %s" % (f, dst))


def putx(f, dst, fav_node):
    print("Upload %s to %s at %s" % (f, dst, fav_node))
    proc = run("./bin/hdfs dfs -putx -f %s %s %s" % (f, dst, fav_node))
    stdout, stderr = proc.communicate()
    while b"were specified but not chosen" in stdout:
        print("retry allocating table")
        proc = run("./bin/hdfs dfs -putx -f %s %s %s" % (f, dst, fav_node))
        stdout, stderr = proc.communicate()
    # print(stdout, stderr)
    return proc


def mkdir(p):
    print("Creating HDFS directory: " + p)
    return run("./bin/hdfs dfs -mkdir -p %s" % p)


def parse_config():
    print("Parsing config file")
    return ConfigFactory.parse_file('../conf/application.conf')


def dbgen(scale):
    print("Generating DB with scale " + str(scale))
    p = subprocess.Popen(
        "./dbgen -qf -s " + str(scale), shell=True, cwd="../dbgen/")
    p.wait()


def key_by_val(table_dict, value):
    for key, val in table_dict.items():
        if value in val:
            return key
    return None


def main():
    if not path.exists(os.environ["HADOOP_HOME"]):
        print("Please Specify HADOOP_HOME")
        exit(0)

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', action='store_true', default=False, 
                        help="Multi-node mode")
    parser.add_argument('-u', action='store_true', default=False, 
                        help="Reuse existing dbgen data")

    opts = parser.parse_args()

    conf = parse_config()
    input_dir = conf.get_string("all.input-dir")
    dbgen_path = path.abspath("../dbgen/")

    try:
        for data_scale in range(INIT_SF, SF+1):

            if not opts.u:
                dbgen(data_scale)

            procs = []
            for d in RAW_DATA:
                p = mkdir(input_dir + "/" + d + "-" + str(data_scale))
                procs.append(p)

            [p.wait() for p in procs]

            procs = []

            if opts.m:
                for d in RAW_DATA:
                    node = key_by_val(DST, d)
                    if node is None:
                        print("Error: " + d + " does not exist!")
                        exit(0)

                    p = putx(path.join(dbgen_path, d + ".tbl"),
                            input_dir + "/" + d + "-" + str(data_scale)
                            + "/" + d + "-" + str(data_scale) + ".txt", 
                            node)
                    procs.append(p)
            else:
                for d in RAW_DATA:
                    p = put(path.join(dbgen_path, d + ".tbl"),
                            input_dir + "/" + d + "-" + str(data_scale)
                            + "/" + d + "-" + str(data_scale) + ".txt")
                    procs.append(p)

            [p.wait() for p in procs]
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
        exit(0)


if __name__ == '__main__':
    main()
