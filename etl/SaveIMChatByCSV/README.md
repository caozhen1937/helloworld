#ʹ��˵��

###��IM����������ת�浽HDFS����json����ȡ�ֶΣ������csv��ʽ�洢��

IMToCsvByCore.java  ʹ��sparkcore
IMToCsvBySQL.java   ʹ��sparkSQL

###�������
��Ҫ�������������ֱ������ݵ�����·��inputPath�����·��outputPath

./spark-submit --master mesos://cdp-bj1-mesos-slave3:5050  --driver-memory 5g  --executor-memory 5g    --executor-cores 1  --num-executors 3 --jars  /home/easemob/caozhen/libs/commons-beanutils-1.7.0.jar,/home/easemob/caozhen/libs/commons-collections-3.1.jar,/home/easemob/caozhen/libs/commons-lang-2.5.jar,/home/easemob/caozhen/libs/commons-logging.jar,/home/easemob/caozhen/libs/ezmorph-1.0.3.jar,/home/easemob/caozhen/libs/json-lib-2.1-jdk15.jar --class com.easemob.caozhen.IMToCsvBySQL /home/easemob/caozhen/jsonToCsv.jar 















