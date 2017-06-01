# tpch-spark

TPC-H queries implemented in Spark using the DataFrames API.
Tested under Spark 2.0.0

Savvas Savvides

ssavvides@us.ibm.com

savvas@purdue.edu


### Todo

- [ ] Clean tables
- [x] Load tables specifically
- [ ] App Name
- [ ] Independent SQL scripts

### Pre-run

Setup env `HADOOP_HOME` as:

```shell
export HADOOP_HOME=/home/ubuntu/hadoop-2.7.3
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
