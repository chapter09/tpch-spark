
#max=22
#for i in seq 3 $max`
#for i in 2 3 5 7 8 9 10 11 18 21
for i in 5 7 8 9 10 11 18 21
do
  echo "Running Query-$i"
  spark-submit --class "main.scala.TpchQuery" ../target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar ../conf/application-q$i.conf
  sleep 2
done
