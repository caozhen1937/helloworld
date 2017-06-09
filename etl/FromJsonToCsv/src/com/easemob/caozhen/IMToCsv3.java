package com.easemob.caozhen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class IMToCsv3 {
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setMaster("mesos://cdp-bj1-mesos-slave3:5050").setAppName("toCSV");
//		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("toCSV");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SparkSession sparkSession = SparkSession.builder().config(sparkConf)
				.getOrCreate();
//		JavaRDD<String> javaRDD = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/user/mayuchao/im-bytes/aeg-00000");
		JavaRDD<String> javaRDD = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/im-msg-bytes-tail");
		
		JavaRDD<List<JSONArray>> flatMap = javaRDD.flatMap(new FlatMapFunction<String, List<JSONArray>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<List<JSONArray>> call(String line) throws Exception {
				String[] split = line.split("\t");
				JSONObject jsonObject = JSONObject.fromObject(split[1]);
				JSONObject jsonObject2 = jsonObject.getJSONObject(split[0]);
				JSONArray jsonArray = jsonObject2.getJSONArray("columns");
				
				//将所有的jsonArray放进list1集合
				List<JSONArray> list1 = new ArrayList<JSONArray>();
				List<List<JSONArray>> list2 = new ArrayList<List<JSONArray>>();
				for(int i=0; i<jsonArray.size();i++){
					list1.add(jsonArray.getJSONArray(i));
				}
				
				int i = 0;
				int count = 8;
				while(i < list1.size()){
					List<JSONArray> sublist =list1.subList(i, count); 		//按照8个jsonArray一条记录切分
					list2.add(sublist);
					
					i=count;
					count+=8;
				}
				return list2.iterator();
			}
		});
		JavaRDD<Row> rowRDD = flatMap.map(new Function<List<JSONArray>, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(List<JSONArray> sublist) throws Exception {
				
				String ts =null;
				String companyname =null;
				String appname =null;
				String msgid =null;
				String msg[] = new String[7];
				List<String> msg1 = new ArrayList<String>();
				for (int j = 0; j <sublist.size(); j++) {
					//jsonArray的第一行转码并去除Unicode字符
					String row1 = Utils.unicode2String(sublist.get(2).getString(0));
					String row = Utils.unicode2String(sublist.get(j).getString(0));
					
					String row2 = sublist.get(j).getString(1);
					
					companyname =row1.split(" ")[1];
					appname =row1.split(" ")[2];
					msgid =row1.split(" ")[3];
					ts = sublist.get(2).getString(2);
					
					//将字段key对应好value
					String ms[] = new String[]{"chattype","direction","from_user","msgbody","msgtimestamp","msgtype","to_user"}; 
					for(int m =0;m<ms.length; m++){
						if(row.indexOf(ms[m]) > -1){
							if(ms[m].equals("msgtimestamp")){
								msg[4] =  Long.valueOf(row2,16).toString();
								
							}else
							if(ms[m].equals("msgbody")){
								msg[3] = new String(Utils.decodeHex(row2.toCharArray()),"UTF-8").replace(",", "，").replace("\n", " ");
							}else {
								msg[m] = new String(Utils.decodeHex(row2.toCharArray()),"UTF-8");   //时间戳不需要转码，所以其他字段在这里处理转码
							}
						}
					}
				}
				
				return RowFactory.create(companyname,appname,msgid,msg[0],msg[1],msg[2],msg[3],msg[4],msg[5],msg[6],ts);
			}

		});
		
//		sparkSession
		ArrayList<StructField> structFields = new ArrayList<StructField>();
		String[] item = new String[] { "companyname","appname","msgid","chattype","direction","from_user","msgbody","msgtimestamp","msgtype","to_user","create_time"};
		for (int i = 0; i < item.length; i++) {
			structFields.add(DataTypes.createStructField(item[i], DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(structFields);
		Dataset<Row> df = sparkSession.createDataFrame(rowRDD, schema);
		df.write().format("com.databricks.spark.csv")
					.option("header",true)
					.save("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/TestJsonToCsv");
	}
}
