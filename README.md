# tpch-spark

TPC-H queries implemented in Spark using the DataFrames API.
Tested under Spark 2.0.0

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [tpch-spark](#tpch-spark)
    - [Todo](#todo)
    - [Pre-run](#pre-run)
      - [Generate dataset](#generate-dataset)
      - [Upload to HDFS](#upload-to-hdfs)
    - [Running](#running)
    - [Application.conf](#applicationconf)
    - [Other Implementations](#other-implementations)
    - [Acknowledgements](#acknowledgements)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Todo

- [ ] Clean tables
- [x] Load tables specifically
- [ ] App Name
- [ ] Independent SQL scripts

### Pre-run

Setup env as below:

```shell
export HADOOP_HOME=/home/ubuntu/hadoop-2.7.3
PATH=$PATH:$HADOOP_HOME/bin/
export SPARK_HOME=/home/ubuntu/spark-2.0.2
PATH=$PATH:$SPARK_HOME/bin/
```

#### Generate dataset

Under `./scripts`

```python
sudo pip3 install -r requirements.txt
```

#### Upload to HDFS

A `Scale Factor` is defined as `SF` in `upload-to-hdfs.py`. 
For example, if `SF` is set as 4, then the script will create data sets from 
scale 1 to scale 4.

Under `./scripts`

```python
chmod +x upload-to-hdfs.py
./upload-to-hdfs.py
```
Since the Hive reads a table as this pattern `path/table-name/table-name.txt`,
the uploaded tables will be kept as the example below:

```shell
/tpch/customer-1/customer-1.txt
```


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
  app-suffix: "1-4-4-4-lineitem-order" //data_scale-exe#-cpu#-mem#-table1-table2
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



### Acknowledgements

This original repo is thanks to Savvas Savvides (ssavvides@us.ibm.com, savvas@purdue.edu)                                                            
