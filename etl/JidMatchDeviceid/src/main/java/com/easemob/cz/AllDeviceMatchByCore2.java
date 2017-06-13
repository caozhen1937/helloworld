package com.easemob.cz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
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

public class AllDeviceMatchByCore2 {
	
	public static void main(String[] args) {
//		SparkConf sparkConf = new SparkConf().setMaster("mesos://cdp-bj1-mesos-slave3:5050").setAppName("sparkCore");
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("sparkCore");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
		
//		JavaRDD<String> javaRDD1 = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/sources/im/taikang_device/tk_device.log", 1);
//		JavaRDD<String> javaRDD2 = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/sources/im/taikang_device/web*usergrid-access*", 1);
//		JavaRDD<String> javaRDD3 = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/sources/im/taikang_device/web*post-token-access*", 1);

		JavaRDD<String> javaRDD1 = sc.textFile("device.txt", 1);
		JavaRDD<String> javaRDD2 = sc.textFile("usergrid.txt", 1);
		JavaRDD<String> javaRDD3 = sc.textFile("post.txt", 1);
		
		
		JavaPairRDD<String,String> deviceRDD = getDeviceRDD(javaRDD1);
		JavaPairRDD<String,String> usergridRDD = getUsergridRDD(javaRDD2);
		JavaPairRDD<String,String> postRDD = getPostRDD(javaRDD3);
		
		saveDeviceInfoWithCsv(deviceRDD,sparkSession);
		saveUsergridAccessWithCsv(usergridRDD,sparkSession);
		savePostAccessWithCsv(postRDD,sparkSession);
		
		joinAndSaveWithCsv(deviceRDD,usergridRDD,postRDD,sparkSession);
		
	}

	private static void joinAndSaveWithCsv(JavaPairRDD<String,String> deviceRDD, JavaPairRDD<String,String> usergridRDD, JavaPairRDD<String,String> postRDD, SparkSession sparkSession) {
		
		JavaPairRDD<String,Tuple2<String,String>> joinRDD = deviceRDD.join(usergridRDD);
		
		JavaPairRDD<String, Tuple2<String, String>> joinRDD2 = joinRDD.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,String>>, String, String>() {
			private static final long serialVersionUID = 1L;
			
			public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> tuple) throws Exception {
				String ip = tuple._2._2.split(",")[0];
				return new Tuple2<String, String>(ip + "_" + tuple._1 , tuple._2._1 + "," + tuple._2._2);
			}
		}).join(postRDD);
		/*
		 * 对最终的RDD进行整理，组织出easemobId， 
		 * 为什么？在这处理数据量小，筛掉了一些错误数据
		 */
		JavaPairRDD<String, String> dfRDD = joinRDD2.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,String>>, String, String>() {
			private static final long serialVersionUID = 1L;
			
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
			
			public Row call(Tuple2<String, String> tuple) throws Exception {
				String[] split1 = tuple._1.split("_");
				String[] split2 = split1[2].split(",");
				String[] split3 = tuple._2.split(",");
				
				return RowFactory.create(split1[0],split3[13],split2[1],split2[2],split2[3],
						split3[0],split3[1],split3[2],split3[3],split3[4],split3[5],split3[6],split3[7],
						split3[8],split3[9],split3[10],split3[11],split3[12]);
			}
		});
		
		ArrayList<StructField> structFields = new ArrayList<StructField>();
		String[] item2 = new String[]{"ip","ts","easemobId","org","app","uuid","type","created","modified","token","model",
				"manufacturer","appKey","version","imei","loc.lat","loc.lng","operator"};
		for(int i=0;i<item2.length;i++){
			structFields.add(DataTypes.createStructField(item2[i], DataTypes.StringType, true));
		}
		//构建StructType，用于最后DataFrame元数据的描述
		StructType schema = DataTypes.createStructType(structFields);
		//基于已有的MetaData以及RDD<Row> 来构造DataFrame
		Dataset<Row> df = sparkSession.createDataFrame(rowRDD2, schema);
		
//		df.write().json("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output9");
//		df.write().csv("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output40");
		df.write().csv("/output56");
	}


	private static void savePostAccessWithCsv(JavaPairRDD<String, String> postRDD, SparkSession sparkSession) {
		JavaRDD<Row> rowRDD = postRDD.map(new Function<Tuple2<String,String>, Row>() {
			
			public Row call(Tuple2<String, String> tuple) throws Exception {
				String[] split2 = tuple._2.split("-");
				return RowFactory.create(split2[0],split2[1],split2[2],split2[3],split2[split2.length-1]);
			}
		});
		
		ArrayList<StructField> structFields = new ArrayList<StructField>();
		String[] item = new String[]{"ip","ts","org","app","easemobId"};
		for(int i=0;i<item.length;i++){
			structFields.add(DataTypes.createStructField(item[i], DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(structFields);
		Dataset<Row> df = sparkSession.createDataFrame(rowRDD, schema);
		
//		df.write().csv("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output38");
//		df.write().csv("/output53");
		
	}

	private static void saveUsergridAccessWithCsv(JavaPairRDD<String, String> usergridRDD, SparkSession sparkSession) {
		
		JavaRDD<Row> rowRDD = usergridRDD.map(new Function<Tuple2<String,String>, Row>() {

			
			public Row call(Tuple2<String, String> tuple) throws Exception {
				String[] split1 = tuple._2.split(",");
				return RowFactory.create(split1[0],split1[1],split1[2],split1[3],split1[4]);
			}
		});
		ArrayList<StructField> structFields = new ArrayList<StructField>();
		String[] item = new String[]{"ip","ts","org","app","devices"};
		for(int i=0;i<item.length;i++){
			structFields.add(DataTypes.createStructField(item[i], DataTypes.StringType, true));
			}
		StructType schema = DataTypes.createStructType(structFields);
		Dataset<Row> df = sparkSession.createDataFrame(rowRDD, schema);
	
//		df.write().csv("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output37");
//		df.write().csv("/output52");
		
	}

	private static void saveDeviceInfoWithCsv(JavaPairRDD<String, String> deviceRDD, SparkSession sparkSession) {
		
		JavaRDD<Row> rowRDD = deviceRDD.map(new Function<Tuple2<String,String>, Row>() {
			
			public Row call(Tuple2<String, String> tuple) throws Exception {
				String[] split3 = tuple._2.split(",");
				return RowFactory.create(split3[13],split3[0],split3[1],split3[2],split3[3],split3[4],split3[5],
						split3[6],split3[7],split3[8],split3[9],split3[10],split3[11],split3[12]);

			}
		});
		ArrayList<StructField> structFields = new ArrayList<StructField>();
		String[] item = new String[]{"ts","uuid","type","created","modified","token","model",
				"manufacturer","appKey","version","imei","loc.lat","loc.lng","operator"};
		for(int i=0;i<item.length;i++){
			structFields.add(DataTypes.createStructField(item[i], DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(structFields);
		Dataset<Row> df = sparkSession.createDataFrame(rowRDD, schema);
		
//		df.write().csv("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output30");
//		df.write().csv("/output51");
		
	}

	private static JavaPairRDD<String,String> getPostRDD(JavaRDD<String> javaRDD3) {
		JavaPairRDD<String, String> postRDD = javaRDD3.mapToPair(new PairFunction<String, String, String>() {

			private static final long serialVersionUID = 1L;

			
			public Tuple2<String, String> call(String line) throws Exception {

				String[] split = line.split(" ");
				String ip = split[0];
				String ts_1 = TimeUtils.toSeconds(split[2]).toString();
				String ts_5 = TimeUtils.toSeconds2(split[2]).toString();
				String org = split[4].split("/")[1];
				String app = split[4].split("/")[2];
				String easemobInfo = split[split.length-1];
				String newline = ip + "-" + ts_1 + "-" + org + "-" + app + "-" + easemobInfo;
				return new Tuple2<String, String>(ip + "_" + ts_1 + "_"+ org, newline);
			}
		});
		
		return postRDD;
	}

	/*
	 * 从文件中获取并清洗数据
	 */
	private static JavaPairRDD<String,String> getUsergridRDD(JavaRDD<String> javaRDD2) {
		
		JavaPairRDD<String, String> usergridRDD = javaRDD2.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			
			public Boolean call(String line) throws Exception {
				if (line.split(" ")[4].indexOf("GET") > -1) {
					return false;
				}
				return true;
			}
		}).filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			
			public Boolean call(String arg0) throws Exception {
				if (arg0.split(" ")[5].indexOf("/") > -1 && arg0.indexOf("devices") > -1) {
//				if (arg0.split(" ")[5].indexOf("/") > -1 && arg0.indexOf("devices") > -1&& arg0.indexOf("4000095522") > -1) {
					return true;
				}
				return false;
			}
		}).mapToPair(new PairFunction<String, String, String>() {

			private static final long serialVersionUID = 1L;

			
			public Tuple2<String, String> call(String line) throws Exception {
				
				String[] split = line.split(" ");
				String ip = split[0];
				String ts_1 = TimeUtils.toSeconds(split[3]).toString();
				String ts_5 = TimeUtils.toSeconds2(split[3]).toString();
				String[] split2 = split[5].split("/");
				String org =null;
				String app =null;
				String devices=null;
				if (split2.length==4){
					org = split[5].split("/")[1];
					app = split[5].split("/")[2];
					devices = split[5].split("/")[3];
				}
//				String taken =split[5];
				String newline = ip + "," + ts_1 + "," + org+ "," + app+ "," +devices;

				return new Tuple2<String, String>(ts_1 + "_" + org, newline);
			}
		});

		return usergridRDD;
	}

	private static JavaPairRDD<String,String> getDeviceRDD(JavaRDD<String> javaRDD1) {
		JavaPairRDD<String, String> deviceRDD = javaRDD1.mapToPair(new PairFunction<String, String, String>() {

			Map<String, String> allInfo = new HashMap<String, String>();
			private static final long serialVersionUID = 1L;

			
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
								
				long ts1_1 = Long.valueOf(created).longValue()/1000;
				String ts1_2 = Long.valueOf(ts1_1).toString();
				
				long ts5_1 = ts1_1/5;							//时间精确度
				String ts5_2 = Long.valueOf(ts5_1).toString();
				
				String newline = uuid + "," +type+ "," +created + ","+ modified+ ","+token + ","+model + ","
						+manufacturer + ","+appKey + "," +version + ","+imei+ ","
				+loc_lat+ "," +loc_lng+ "," +operator+ ","+ ts1_2;
				return new Tuple2<String, String>(ts1_2 + "_" + appKey.split("/")[0], newline);
			}
		});
		
		return deviceRDD;
	}
	
	
}
