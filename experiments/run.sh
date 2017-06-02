#!/usr/bin/bash

max=11
for i in `seq 1 $max`
do
  spark-submit --class "main.scala.TpchQuery" --master spark://10.12.3.36:7077 target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar
  sleep 10
done

