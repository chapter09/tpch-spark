## Usages of Scripts 

#### Before you run

Modify the config file at `../conf/application.conf`.

#### Install dependencies

```
sudo pip3 install -r requirements.txt
```

#### upload-to-hdfs.py 

For single node:

```
./upload-to-hdfs.py
```

For multiple nodes (when you need to assign storage node):
```
upload-to-hdfs.py -m
```



#### tc.sh usage
This is a local script for setting bandwidth limitations. 

```
tc.sh <dst_ip> <bw>
```

To set the bandwdith to the dst IP as 10Mbps, format the command as below:

```
tc.sh dist_IP 10
```
