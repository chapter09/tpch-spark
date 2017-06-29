#!/usr/bin/python3

import subprocess
import time
import subprocess
from pyhocon import ConfigFactory
from pyhocon import HOCONConverter 
from collections import OrderedDict
import argparse

TEST = True
(EXE, CPU, MEM, BW) = (2, 8, 8, 100)
ROUND = 3
SF = 5
QUERYS = {
    "customer-orders": "SELECT * FROM order INNER JOIN customer " 
    + "ON customer.c_custkey=order.o_custkey",
    "lineitem-orders": "SELECT * FROM lineitem INNER JOIN order "
    + "ON lineitem.l_orderkey=order.o_orderkey",
    "nation-customer": "SELECT * FROM nation INNER JOIN customer "
    + "ON nation.n_nationkey=customer.c_nationkey",
    "part-partsupp": "SELECT * FROM part INNER JOIN partsupp "
    + "ON partsupp.ps_partkey=part.p_partkey",
    "partsupp-lineitem": "SELECT * FROM lineitem INNER JOIN partsupp "
    + "ON partsupp.ps_suppkey=lineitem.l_suppkey "
    + "AND partsupp.ps_partkey=lineitem.l_partkey",
    "partsupp-supplier": "SELECT * FROM supplier INNER JOIN partsupp "
    + "ON partsupp.ps_suppkey=supplier.s_suppkey",
    "region-nation": "SELECT * FROM region INNER JOIN nation "
    + "ON region.r_regionkey=nation.n_regionkey",
    #"supplier-customer": "SELECT * FROM supplier INNER JOIN customer "
    #+ "ON supplier.s_nationkey=customer.c_nationkey",
    "supplier-nation": "SELECT * FROM supplier INNER JOIN nation "
    + "ON nation.n_nationkey=supplier.s_nationkey" 
}


def gen_conf(t1, t2, scale, app_name, query):
    conf = ConfigFactory.parse_file('../conf/application.conf')
    conf.put("Q23.table-list", [t1, t2])
    conf.put("all.data-scale", scale)
    conf.put("all.app-suffix", app_name)
    conf.put("Q23.query", query)

    with open('../conf/application-run.conf', 'w') as f:
       f.write(HOCONConverter.convert(conf, 'hocon'))


def run(cmd):
    return subprocess.run(cmd, shell=True)


def main():
    try:
        for sf in range(1, SF + 1):
            for q in QUERYS:
                t1, t2 = q.split('-')

                if TEST:
                    app_name = "test-%d-%d-%d-%d-%d-%s-%s" % (
                        sf, EXE, CPU, MEM, BW, t1, t2) 
                else:
                    app_name = "%d-%d-%d-%d-%d-%s-%s" % (
                        sf, EXE, CPU, MEM, BW, t1, t2) 

                # Generate application-run.conf
                gen_conf(t1, t2, sf, app_name, q)
                for i in range(0, ROUND):
                    print("RUN " + app_name + " ROUND #" + str(i))
                    cmd = "spark-submit --class \"main.scala.TpchQuery\"" \
                    + " ../target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar" \
                    + " ../conf/application-run.conf" 
                    print(cmd)
                    run(cmd)
                    exit(0)
                    time.sleep(10)
    except KeyboardInterrupt:
        print("Keyboard Interrupt")
        exit(0)


if __name__ == "__main__":
    main()

