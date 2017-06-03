
max=11
for i in `seq 1 $max`
do
  #spark-submit --class "main.scala.TpchQuery" target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar experiments/3/supplier-customer.conf
  spark-submit --class "main.scala.TpchQuery" target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar experiments/3/supplier-nation.conf
  sleep 10
done
