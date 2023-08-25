
# 准备

## 1. 整体打包

```sh
mvn clean package -pl distribution -am -DskipTests
```

## 2. 支持HDFS

* 打包

```sh
mvn clean package -pl iotdb-core/datanode,iotdb-connector/hadoop -am -Dmaven.test.skip=true -P get-jar-with-dependencies
```
然后将 `iotdb-connector/hadoop/target/hadoop-tsfile-1.2.0-SNAPSHOT-jar-with-dependencies.jar` 拷贝到主lib目录中

* 修改 `conf/iotdb-datanode.properties` 配置文件

> 这里可以参考官网文档（但是有部分错误）: https://iotdb.apache.org/UserGuide/V1.1.x/Ecosystem-Integration/Writing-Data-on-HDFS.html#integration-with-hdfs

```properties
# 文件修改成hdfs协议
dn_data_dirs=hdfs://HDFS8000071/iotdb/node0/datanode/data
enable_hdfs=true
tsfile_storage_fs=HDFS
core_site_path=/usr/local/service/hadoop/etc/hadoop/core-site.xml
hdfs_site_path=/usr/local/service/hadoop/etc/hadoop/hdfs-site.xml
# 支持集群模式
hdfs_nameservices=HDFS8000071
dfs_nameservices=HDFS8000071
hdfs_ha_namenodes=nn1,nn2
dfs_ha_namenodes=nn1,nn2
hdfs_ip=10.134.180.44,10.134.180.145
hdfs_port=4007
dfs_ha_automatic_failover_enabled=true
dfs_client_failover_proxy_provider=org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
```

* 修改 `sbin/start-datanode.sh` 脚本中的内容

```sh
# 使用实际hadoop配置路径
CLASSPATH="/usr/local/service/hadoop/etc/hadoop"
```

## 大内存机型优化
建议使用java17及后续版本

java17 优化参数
```
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+UseG1GC"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:MaxGCPauseMillis=5000"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:ParallelGCThreads=64"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:ConcGCThreads=16"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:G1HeapRegionSize=32M"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:+UseStringDeduplication"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -XX:InitiatingHeapOccupancyPercent=80"
IOTDB_JMX_OPTS="$IOTDB_JMX_OPTS -Xlog:gc*=info,gc+heap=trace,gc+ref*=debug,age*=debug,gc+stringdedup*=debug:file=/iotdb/jvm/logs/gc.log:time,level,pid,tags:filecount=3,filesize=100M"
```
