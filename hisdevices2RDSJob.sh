#!/bin/bash
#参数 起始日期、终止日期
startdate=`date -d "+0 day $1" +%Y%m%d`
enddate=`date -d "+0 day $2" +%Y%m%d`

while [[ $startdate -le $enddate ]]
do
date=$startdate

nohup bash /home/easemob/apps/opt/spark/spark/bin/spark-submit \
--class com.easemob.toRDS.Deviceinfo2Table \
--master mesos://zk://cdp-bj1-mesos-slave1:2181,cdp-bj1-mesos-slave2:2181,cdp-bj1-mesos-slave3:2181/mesos \
--executor-memory 6g \
--total-executor-cores 9 \
--jars /home/easemob/caozhen/libs/mysql-connector-java-5.1.26-bin.jar \
/home/easemob/caozhen/DeviceETL2RDS.jar \
hdfs://cdp-bj1-hdfs-server1:9000/sources/im/kafka/im-user-statistic/$date*/* hdfs://cdp-bj1-hdfs-server1:9000/sources/cdp/base/deviceidAndOSversion/ hdfs://cdp-bj1-hdfs-server1:9000/sources/cdp/base/deviceAndApp  >  /home/easemob/apps/log/DeviceETL2RDS/$date.log 2>&1

echo "$date"
startdate=`date -d "+1 day $date" +%Y%m%d`

done

