package com.easemob.cz;

/*
 * ./spark-submit --master mesos://cdp-bj1-mesos-slave3:5050 --executor-memory 12g --executor-cores 2 --jars  /home/easemob/caozhen/libs/commons-beanutils-1.7.0.jar,/home/easemob/caozhen/libs/commons-collections-3.1.jar,/home/easemob/caozhen/libs/commons-lang-2.5.jar,/home/easemob/caozhen/libs/commons-logging.jar,/home/easemob/caozhen/libs/ezmorph-1.0.3.jar,/home/easemob/caozhen/libs/json-lib-2.1-jdk15.jar --class com.easemob.cz.Test1 /home/easemob/caozhen/device.jar 
 *
 */
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.sf.json.JSONObject;
import scala.Tuple2;

public class TkongDevice {

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setMaster("mesos://cdp-bj1-mesos-slave3:5050").setAppName("aa");
		
//		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("aa");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		SQLContext sqlContext = new SQLContext(sc);
		JavaRDD<String> javaRDD1 = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/sources/im/taikang_device/tk_device.log", 1);
		JavaRDD<String> javaRDD2 = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/sources/im/taikang_device/web*usergrid-access*", 1);
		JavaRDD<String> javaRDD3 = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/sources/im/taikang_device/web*post-token-access*", 1);

//		JavaRDD<String> javaRDD1 = sc.textFile("device.txt", 1);
//		JavaRDD<String> javaRDD2 = sc.textFile("usergrid.txt", 1);
//		JavaRDD<String> javaRDD3 = sc.textFile("post.txt", 1);
		
		final Long time1 = (long) 1490371200;  //1s
		final Long time2 = (long) 1490457599;
	
		final Long time3 = (long) 298074240;  //5s
		final Long time4 = (long) 298091520;
			
		// 处理日志 tk_device.log
		JavaPairRDD<String, String> deviceRDD = javaRDD1.mapToPair(new PairFunction<String, String, String>() {

			Map<String, String> allInfo = new HashMap<String, String>();
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String line) throws Exception {

				
				String str = line.substring(92);
				String[] split = str.split(", ");

				for (int i = 0; i < split.length; i++) {

					if (split[i].indexOf("=") > -1) {

						if (split[i].indexOf("}") > -1) {
							String[] split1 = split[i].split("}")[0].split("=");
							allInfo.put(split1[0], split1[1]);
						} else {
							String[] split2 = split[i].split("=");
							allInfo.put(split2[0], split2[1]);
						}

					}
				}
				
				
//				Set<Entry<String, String>> entrySet = allInfo.entrySet();
//				for (Entry<String, String> entry : entrySet) {
//					newline = newline + entry.getValue() + ",";
//				}
				String uuid = allInfo.get("uuid");
				String type = allInfo.get("type");
				String created = allInfo.get("created");
				String modified = allInfo.get("modified");
				String token = allInfo.get("token");
				String model = allInfo.get("model");
				String manufacturer = allInfo.get("manufacturer");
				String appKey = allInfo.get("appKey");
				String version = allInfo.get("version");
				
				String imei ="null";
				if(allInfo.get("imei")!=null){
					imei = allInfo.get("imei");
				}
				String loc_lat ="null";
				if(allInfo.get("loc.lat")!=null){
					loc_lat = allInfo.get("loc.lat");
				}
				String loc_lng ="null";
				if(allInfo.get("loc.lng")!=null){
					loc_lng = allInfo.get("loc.lng");
				}
				String operator ="null";
				if(allInfo.get("operator")!=null){
					operator = allInfo.get("operator");
				}
								
				long ts1 = Long.valueOf(created).longValue()/1000;
				String ts3 = Long.valueOf(ts1).toString();
				
				long ts2 = ts1/5;//时间精确度
				String ts = Long.valueOf(ts2).toString();
				
				String newline = uuid + "," +type+ "," +created + ","+ modified+ ","+token + ","+model + ","
						+manufacturer + ","+appKey + "," +version + ","+imei+ ","
				+loc_lat+ "," +loc_lng+ "," +operator+ ","+ ts3;
				return new Tuple2<String, String>(ts3, newline);
			}
		}).filter(new Function<Tuple2<String,String>, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				Long ts =Long.valueOf(tuple._1).longValue();
				if(ts>=time1 && ts<=time2){
					return true;
				}
				return false;
			}
		});
		JavaPairRDD<String, String> deviceCacheRDD = deviceRDD.cache();
		
		JavaRDD<Row> rowRDD3 = deviceCacheRDD.map(new Function<Tuple2<String,String>, Row>() {

			@Override
			public Row call(Tuple2<String, String> tuple) throws Exception {
				String[] split3 = tuple._2.split(",");
				String uuid = split3[0];
				String type = split3[1];
				String created = split3[2];
				String modified = split3[3];
				String token = split3[4];
				String model = split3[5];
				String manufacturer = split3[6];
				String appKey = split3[7];
				String version = split3[8];
				String imei = split3[9];
				String loc_lat = split3[10];
				String loc_lng = split3[11];
				String operator = split3[12];
				String ts = split3[13];
				return RowFactory.create(ts,uuid,type,created,modified,token,model,manufacturer,appKey,version,imei,loc_lat,loc_lng,operator);

			}
		});
		ArrayList<StructField> structFields3 = new ArrayList<StructField>();
		String[] item3 = new String[]{"ts","uuid","type","created","modified","token","model",
				"manufacturer","appKey","version","imei","loc.lat","loc.lng","operator"};
		for(int i=0;i<item3.length;i++){
			structFields3.add(DataTypes.createStructField(item3[i], DataTypes.StringType, true));
			}
		StructType schema3 = DataTypes.createStructType(structFields3);
		
		Dataset<Row> df3 = sqlContext.createDataFrame(rowRDD3, schema3);
		
		df3.write().csv("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output30");
//		deviceCacheRDD.saveAsTextFile("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output30");
		
		
		
		
		// 处理日志usergrid-access.log_20170325
		// 过滤了GET请求
		JavaPairRDD<String, String> usergridRDD = javaRDD2.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String line) throws Exception {
				if (line.split(" ")[4].indexOf("GET") > -1) {
					return false;
				}
				return true;
			}
		}).filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String arg0) throws Exception {
				if (arg0.split(" ")[5].indexOf("/") > -1 && arg0.indexOf("devices") > -1&& arg0.indexOf("4000095522") > -1) {
					return true;
				}
				return false;
			}
		}).mapToPair(new PairFunction<String, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String line) throws Exception {

				
				String[] split = line.split(" ");
				String ip = split[0];
				String ts = TimeUtils.toSeconds(split[3]).toString();
				String ts1 = TimeUtils.toSeconds2(split[3]).toString();
				String[] split2 = split[5].split("/");
				String org =null;
				String app =null;
				String tKong=null;
				if (split2.length==4){
					org = split[5].split("/")[1];
					app = split[5].split("/")[2];
					tKong = split[5].split("/")[3];
				}
//				String taken =split[5];
				String newline = ip + "," + ts + "," + org+ "," + app+ "," +tKong;

				return new Tuple2<String, String>(ts1, newline);
			}
		}).filter(new Function<Tuple2<String,String>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				Long ts =Long.valueOf(tuple._1).longValue();
				if(ts>=time3 && ts<=time4){
					return true;
				}
				return false;
			}
		});
		JavaPairRDD<String, String> usergridCacheRDD = usergridRDD.cache();
		
		JavaRDD<Row> rowRDD4 = usergridCacheRDD.map(new Function<Tuple2<String,String>, Row>() {

			@Override
			public Row call(Tuple2<String, String> tuple) throws Exception {
				String[] split2 = tuple._2.split(",");
				String ip = split2[0];
				String ts = split2[1];
				String org = split2[2];
				String app = split2[3];
				String tKong = split2[4];
				return RowFactory.create(ip,ts,org,app,tKong);
			}
		});
		ArrayList<StructField> structFields4 = new ArrayList<StructField>();
		String[] item4 = new String[]{"ip","ts","org","app","tKong"};
		for(int i=0;i<item4.length;i++){
			structFields4.add(DataTypes.createStructField(item4[i], DataTypes.StringType, true));
			}
		StructType schema4 = DataTypes.createStructType(structFields4);
		
		Dataset<Row> df4 = sqlContext.createDataFrame(rowRDD4, schema4);
		
//		df4.write().csv("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output37");
//		usergridCacheRDD.saveAsTextFile("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output22");

		
		
		// 处理日志post-token-access.log_20170325
		JavaPairRDD<String, String> postRDD = javaRDD3.filter(new Function<String, Boolean>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String line) throws Exception {
				if(line.indexOf("4000095522") > -1){
					return true;
				}
				return false;
			}
		}).mapToPair(new PairFunction<String, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String line) throws Exception {

				String[] split = line.split(" ");
				String ip = split[0];
				String ts = TimeUtils.toSeconds(split[2]).toString();
				String ts1 = TimeUtils.toSeconds2(split[2]).toString();
				String org = split[4].split("/")[1];
				String app = split[4].split("/")[2];
				String easemobInfo = split[split.length-1];
				String newline = ip + "-" + ts + "-" + org + "-" + app + "-" + easemobInfo;
				return new Tuple2<String, String>(ip + "_" + ts1, newline);
			}
		}).filter(new Function<Tuple2<String,String>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				Long ts =Long.valueOf(tuple._1.split("_")[1]).longValue();
				if(ts>=time3 && ts<=time4){
					return true;
				}
				return false;
			}
		});
		JavaPairRDD<String, String> postCacheRDD = postRDD.cache();
		
		JavaRDD<Row> rowRDD5 = postCacheRDD.map(new Function<Tuple2<String,String>, Row>() {

			@Override
			public Row call(Tuple2<String, String> tuple) throws Exception {
				String[] split2 = tuple._2.split("-");
				String ip = split2[0];
				String ts = split2[1];
				String org = split2[2];
				String app = split2[3];
				String easemobId = split2[split2.length-1];
				return RowFactory.create(ip,ts,org,app,easemobId);
			}
		});
		
		ArrayList<StructField> structFields5 = new ArrayList<StructField>();
		String[] item5 = new String[]{"ip","ts","org","app","easemobId"};
		for(int i=0;i<item5.length;i++){
			structFields5.add(DataTypes.createStructField(item5[i], DataTypes.StringType, true));
			}
		StructType schema5 = DataTypes.createStructType(structFields5);
		
		Dataset<Row> df5 = sqlContext.createDataFrame(rowRDD5, schema5);
		
//		df5.write().csv("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output38");
//		postCacheRDD.saveAsTextFile("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output32");
		
		
		
		/*
		 * 进行join
		 * 1、先以时间戳为key，join到两个文件，
		 * 2、两个转换成  IP_时间戳 为key的RDD，在进行join
		 */
		JavaPairRDD<String,Tuple2<String,String>> joinRDD = deviceCacheRDD.join(usergridCacheRDD);
		
//		joinRDD.saveAsTextFile("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output24");
		
		JavaRDD<Row> rowRDD1 = joinRDD.map(new Function<Tuple2<String,Tuple2<String,String>>, Row>() {

			@Override
			public Row call(Tuple2<String, Tuple2<String, String>> tuple) throws Exception {
				
				String[] split3 = tuple._2._1.split(",");
				String[] split2 = tuple._2._2.split(",");
				
				String ip = split2[0];
				String ts = split2[1];
				String org = split2[2];
				String app = split2[3];
				String tKong = split2[4];
				
				String uuid = split3[0];
				String type = split3[1];
				String created = split3[2];
				String modified = split3[3];
				String token = split3[4];
				String model = split3[5];
				String manufacturer = split3[6];
				String appKey = split3[7];
				String version = split3[8];
				String imei = split3[9];
				String loc_lat = split3[10];
				String loc_lng = split3[11];
				String operator = split3[12];
				return RowFactory.create(ip,ts,org,app,tKong,
						uuid,type,created,modified,token,model,manufacturer,appKey,version,imei,loc_lat,loc_lng,operator);
			}
		});
		
		ArrayList<StructField> structFields1 = new ArrayList<StructField>();
		
		String[] item1 = new String[]{"ip","ts","org","app","tKong","uuid","type","created","modified","token","model",
				"manufacturer","appKey","version","imei","loc.lat","loc.lng","operator"};
		for(int i=0;i<item1.length;i++){
			structFields1.add(DataTypes.createStructField(item1[i], DataTypes.StringType, true));
			}
//		structFields1.add(DataTypes.createStructField("ip", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("ts", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("org", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("app", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("tKong", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("uuid", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("type", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("created", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("modified", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("token", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("model", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("manufacturer", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("appKey", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("version", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("imei", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("loc.lat", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("loc.lng", DataTypes.StringType, true));
//		structFields1.add(DataTypes.createStructField("operator", DataTypes.StringType, true));
		//构建StructType，用于最后DataFrame元数据的描述
		StructType schema1 = DataTypes.createStructType(structFields1);
		
		Dataset<Row> df1 = sqlContext.createDataFrame(rowRDD1, schema1);
		
//		df1.write().csv("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output39");
		/*
		 * 第一次的joinRDD要整理为IP_时间戳 为key
		 * deviceInfo和user文件中的信息为value
		 * 
		 */
		JavaPairRDD<String, Tuple2<String, String>> joinRDD2 = joinRDD.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,String>>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> tuple) throws Exception {
				String ip = tuple._2._2.split(",")[0];
				return new Tuple2<String, String>(ip + "_" + tuple._1 , tuple._2._1 + "," + tuple._2._2);
			}
		}).join(postCacheRDD);
		/*
		 * 对最终的RDD进行整理，组织出easemobId， 
		 * 为什么？在这处理数据量小，筛掉了一些错误数据
		 */
		JavaPairRDD<String, String> dfRDD = joinRDD2.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,String>>, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> tuple)  {
				
				String easemobInfo = tuple._2._2.split("-")[4].replace("\\x22", "\"").replace("\\x5C", "\\");
				
				String org = tuple._2._2.split("-")[2];
				String app = tuple._2._2.split("-")[3];
				try {
					String easemobId = null;
//					if(tuple._2._2.split(",")[4]!=null){
					if (easemobInfo.indexOf("username") > -1 && easemobInfo.indexOf("{") > -1) {
						JSONObject jsonObject = JSONObject.fromObject(easemobInfo);
						easemobId = jsonObject.getString("username");
					}
					return new Tuple2<String, String>(tuple._1 + "," + easemobId + "," + org + "," + app, tuple._2._1);
//					}
//					return new Tuple2<String, String>(tuple._1 + "," + "FAILD NULL" + ","+ org + "," + app, tuple._2._1);
				} catch (Exception e) {
					return new Tuple2<String, String>(tuple._1 + "," + "FAILD Unterminated" + ","+ org + "," + app, tuple._2._1);

				}
				
			}
	
		});

				
		
		JavaRDD<Row> rowRDD2 = dfRDD.map(new Function<Tuple2<String,String>, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, String> tuple) throws Exception {
				String[] split1 = tuple._1.split("_");
				String[] split2 = split1[1].split(",");
				String ip = split1[0];
				
				
				String easemobId = split2[1];
				String org = split2[2];
				String app = split2[3];
				
				String[] split3 = tuple._2.split(",");
				String uuid = split3[0];
				String type = split3[1];
				String created = split3[2];
				String modified = split3[3];
				String token = split3[4];
				String model = split3[5];
				String manufacturer = split3[6];
				String appKey = split3[7];
				String version = split3[8];
				String imei = split3[9];
				String loc_lat = split3[10];
				String loc_lng = split3[11];
				String operator = split3[12];
				String ts = split3[13];
				
				return RowFactory.create(ip,ts,easemobId,org,app,
						uuid,type,created,modified,token,model,manufacturer,appKey,version,imei,loc_lat,loc_lng,operator);
			}
		});
		
		
		ArrayList<StructField> structFields = new ArrayList<StructField>();
		String[] item2 = new String[]{"ip","ts","easemobId","org","app","uuid","type","created","modified","token","model",
				"manufacturer","appKey","version","imei","loc.lat","loc.lng","operator"};
		for(int i=0;i<item2.length;i++){
			structFields.add(DataTypes.createStructField(item2[i], DataTypes.StringType, true));
			}
//		structFields.add(DataTypes.createStructField("ip", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("ts", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("easemobId", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("org", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("app", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("uuid", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("created", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("modified", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("token", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("model", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("manufacturer", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("appKey", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("version", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("imei", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("loc.lat", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("loc.lng", DataTypes.StringType, true));
//		structFields.add(DataTypes.createStructField("operator", DataTypes.StringType, true));
		//构建StructType，用于最后DataFrame元数据的描述
		StructType schema = DataTypes.createStructType(structFields);
		
		/**
		 * 基于已有的MetaData以及RDD<Row> 来构造DataFrame
		 */
		Dataset<Row> df = sqlContext.createDataFrame(rowRDD2, schema);
		
//		df.write().json("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output9");
//		df.write().csv("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output40");
	}
}
