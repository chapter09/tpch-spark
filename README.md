# tpch-spark

TPC-H queries implemented in Spark using the DataFrames API.
Tested under Spark 2.0.0

Savvas Savvides

ssavvides@us.ibm.com

savvas@purdue.edu


### Todo

- [ ] Clean tables
- [ ] Load tables specifically

### Running

First compile using:

```
sbt assembly 
```
Config in `conf/application.conf`

You can then run a query using:

```
spark-submit --class "main.scala.TpchQuery" --master MASTER target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar
```

Specify `application.conf`

```
spark-submit --class "main.scala.TpchQuery" --master MASTER target/scala-2.11/Spark-TPCH-queries-assembly-1.0.jar path_to_application.conf
```
### Application.conf

```
all {
  hdfs: "hdfs://10.12.3.36:8020/",
  input-dir: "/tpch/",
  output-dir: "/tpch/",
  query-num: 23,
  data-scale: 1
}

// Table list:
// customer, lineitem, nation, region
// order, part, partsupp, supplier

Q23 {
  table-a: "customer",
  table-b: "lineitem",
  operator: "join",
  query: ""
}
```

### Other Implementations

1. Data generator (http://www.tpc.org/tpch/)

2. TPC-H for Hive (https://issues.apache.org/jira/browse/hive-600)

3. TPC-H for PIG (https://github.com/ssavvides/tpch-pig)
