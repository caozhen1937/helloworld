#使用说明

###匹配所有的deviceInfo和easemobID

AllDeviceMatchBySQL.java 是所有数据进行匹配的main方法，是优化后的SQL写的。

###任务命令：
有五个输入参数，分别为：三个文件的输入路径、device的数据表输出、结果的数据表输出

./spark-submit --master mesos://cdp-bj1-mesos-slave3:5050  --driver-memory 2g  --executor-memory 5g    --executor-cores 3  --num-executors 3 --jars  /home/easemob/caozhen/libs/commons-beanutils-1.7.0.jar,/home/easemob/caozhen/libs/commons-collections-3.1.jar,/home/easemob/caozhen/libs/commons-lang-2.5.jar,/home/easemob/caozhen/libs/commons-logging.jar,/home/easemob/caozhen/libs/ezmorph-1.0.3.jar,/home/easemob/caozhen/libs/json-lib-2.1-jdk15.jar --class com.easemob.cz.AllDeviceMatchBySQL /home/easemob/caozhen/device.jar hdfs://cdp-bj1-hdfs-server1:9000/sources/im/taikang_device/tk_device.log hdfs://cdp-bj1-hdfs-server1:9000/sources/im/taikang_device/web*usergrid-access* hdfs://cdp-bj1-hdfs-server1:9000/sources/im/taikang_device/web*post-token-access* hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output777 hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output666

