#!/usr/bin/python3

import subprocess
import time
import subprocess
from pyhocon import ConfigFactory
from pyhocon import HOCONConverter 
from collections import OrderedDict
import sys
import os

TEST = False
SF = 5
HDFS = '10.2.3.20'
(EXE, CPU, MEM, BW) = (2, 8, 8, 500)
ROUND = 3


def gen_conf(scale, app_name, query):
    conf = ConfigFactory.parse_file('../conf/application.conf')
    conf.put("all.data-scale", scale)
    conf.put("all.query-num", query)
    conf.put("all.hdfs", 'hdfs://%s:8020/'%HDFS)
    conf.put("all.app-suffix", app_name)

    with open('../conf/application-run.conf', 'w') as f:
       f.write(HOCONConverter.convert(conf, 'hocon'))

def result_size(app_name, q):
    # KB
    dir_name = "Q%.2d" % q
    proc = run(
        "hdfs dfs -du /tpch/%s | awk '{ SUM += $1 } END {print SUM/1024}'" % dir_name)
    with open("./SIZE.txt", "a") as fd:
        fd.write(app_name + ", " + proc.stdout.decode("utf-8") )


def run(cmd):
    return subprocess.run(cmd, stdout=subprocess.PIPE, 
                          stderr=subprocess.STDOUT, shell=True)


def all():
    try:
        for sf in range(15, 16):
            for q in [2, 3, 5, 7, 8, 9, 10, 11, 18, 21]:
                if TEST:
                    app_name = "Q%.2d-" % q + str(sf) 
                else:
                    app_name = "Q%.2d-" % q + str(sf) 

                # Generate application-run.conf
                gen_conf(sf, app_name, q)
                #gen_conf("nation", "customer", sf, app_name, q)
                print("RUN " + app_name)
                cmd = "spark-submit --class \"main.scala.TpchQuery\"" \
                + " ../target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar" \
                + " ../conf/application-run.conf" 
                print(cmd)
                run(cmd)
                result_size(app_name, q)
                time.sleep(3)
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
        exit(0)


def one_shot(q, scale=6):
    if scale < 6:
        app_name = "Q%.2d-" % q + str(scale) 
        gen_conf(scale, app_name, q)
        #gen_conf("nation", "customer", sf, app_name, q)
        print("RUN " + app_name)
        cmd = "spark-submit --class \"main.scala.TpchQuery\"" \
        + " ../target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar" \
        + " ../conf/application-run.conf" 
        print(cmd)
        run(cmd)
        result_size(app_name, q)
    else:
        for s in range(1, scale):
            app_name = "Q%.2d-" % q + str(s) 
            gen_conf(s, app_name, q)
            #gen_conf("nation", "customer", sf, app_name, q)
            print("RUN " + app_name)
            cmd = "spark-submit --class \"main.scala.TpchQuery\"" \
            + " ../target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar" \
            + " ../conf/application-run.conf" 
            print(cmd)
            run(cmd)
            result_size(app_name, q)
            time.sleep(3)


def main():
    if len(sys.argv) == 1:
        all()
    elif len(sys.argv) == 2:
        one_shot(int(sys.argv[1]))
    else:
        one_shot(int(sys.argv[1]), int(sys.argv[2]))


if __name__ == "__main__":
    main()

