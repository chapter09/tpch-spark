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

DST = {'hao-ml-2': ['lineitem', 'supplier', 'region', 'part'],
       'hao-ml-5': ['customer', 'orders', 'nation', 'partsupp']}

INIT_SF = 6
SF = 10

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

            for d in RAW_DATA:
                if opts.m:
                    if d in list(DST.values())[0]:
                        node = list(DST.keys())[0]
                    elif d in list(DST.values())[1]:
                        node = list(DST.keys())[1]
                    else:
                        print("Error: " + d + "does not exist!")
                        exit(0)
                    p = putx(path.join(dbgen_path, d + ".tbl"),
                            input_dir + "/" + d + "-" + str(data_scale)
                            + "/" + d + "-" + str(data_scale) + ".txt", 
                            node)
                    procs.append(p)
                else:
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
