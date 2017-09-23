
max=22
for i in `seq 1 $max`
do
  echo "Running Query-$i"
  spark-submit --class "main.scala.TpchQuery" ../target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar ../conf/application-q$i.conf
  sleep 2
done
