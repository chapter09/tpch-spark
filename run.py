#!/usr/bin/python3

import subprocess
import time

ROUND = 11
SF = 5
CONFS = [
    "customer-orders.conf",
    "lineitem-orders.conf",
    "nation-customer.conf",
    "part-partsupp.conf",
    "partsupp-lineitem.conf",
    "partsupp-supplier.conf",
    "region-nation.conf",
    #"supplier-customer.conf",
    "supplier-nation.conf"
]

def run(cmd):
    return subprocess.run(cmd, shell=True)

try:
    for sf in range(1, SF + 1):
        for conf in CONFS:
            for i in range(0, ROUND):
                print("RUN " + conf + " ROUND #" + str(i))
                cmd = "spark-submit --class \"main.scala.TpchQuery\"" \
                + " target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar" \
                + " experiments/%d/"%sf + conf
                print(cmd)
                run(cmd)
                time.sleep(10)
except KeyboardInterrupt:
    print("Keyboard Interrupt")
    exit(0)

