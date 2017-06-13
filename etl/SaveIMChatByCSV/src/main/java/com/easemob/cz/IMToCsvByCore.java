package com.easemob.cz;

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

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class IMToCsvByCore {
	public static void main(String[] args) {
		
		String inputPath = args[0];
		String outputPath = args[1];
		
		SparkConf sparkConf = new SparkConf().setMaster("mesos://cdp-bj1-mesos-slave3:5050").setAppName("toCSV");
//		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("toCSV");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
//		JavaRDD<String> javaRDD = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/user/mayuchao/im-bytes/aeg-00000");
//		JavaRDD<String> javaRDD = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/im-msg-bytes-tail");
		JavaRDD<String> javaRDD = sc.textFile(inputPath);
		
		javaRDD.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String line) throws Exception {
				String[] split = line.split("\t");
				JSONObject jsonObject = JSONObject.fromObject(split[1]);
				JSONObject jsonObject2 = jsonObject.getJSONObject(split[0]);
				JSONArray jsonArray = jsonObject2.getJSONArray("columns");
				
				//将所有的jsonArray放进list1集合
				List<JSONArray> list1 = new ArrayList<JSONArray>();
				List<String> list2 = new ArrayList<String>();
				for(int i=0; i<jsonArray.size();i++){
					list1.add(jsonArray.getJSONArray(i));
				}
				
				int i = 0;
				int count = 8;
				while(i < list1.size()){
					List<JSONArray> sublist =list1.subList(i, count); 		//按照8个jsonArray一条记录切分
					
					Map<String,String> map = new HashMap<String,String>();  //取值，并放入map
					String ts =null;
					String companyname =null;
					String appname =null;
					String msgid =null;
					for (int j = 0; j <sublist.size(); j++) {
						//jsonArray的第一行转码并去除Unicode字符
						String row1 = Utils.unicode2String(sublist.get(2).getString(0));
						String row = Utils.unicode2String(sublist.get(j).getString(0));
						
						String row2 = sublist.get(j).getString(1);
						
						if(row1.split(" ").length>=4){
							companyname =row1.split(" ")[1];
							appname =row1.split(" ")[2];
							msgid =row1.split(" ")[3];
						}
						
						ts = sublist.get(j).getString(2);
						
						//将字段key对应好value
						String ms[] = new String[]{"msgbody","msgtype","to_user","chattype","direction","from_user","msgtimestamp"}; 
						for(int m =0;m<ms.length; m++){
							if(row.indexOf(ms[m]) > -1){
								if(ms[m].equals("msgtimestamp")){
									map.put("msgtimestamp",Long.valueOf(row2,16).toString());
								}else{
									map.put(ms[m],new String(Utils.decodeHex(row2.toCharArray()),"UTF-8"));   //时间戳不需要转码，所以其他字段在这里处理转码
								}
							}
						}
					}
					map.put("create_time", ts);
					map.put("companyname", companyname);
					map.put("appname", appname);
					map.put("msgid", msgid);
					
					//输出csv格式
					String newline = map.get("companyname")+ "," + map.get("appname")+ "," + map.get("msgid")+ "," + map.get("chattype")+ ","
							+map.get("direction")+ "," + map.get("from_user")+ ","+  "\"" + map.get("msgbody").replace(",", "，").replace("\n", " ")+ "\""+ ","
							+ map.get("msgtimestamp")+ ","+ map.get("msgtype")+ "," + map.get("to_user")+ ","+ map.get("create_time");
					
					
					list2.add(newline);  //将所有的记录再放入list2中
					i=count;
					count+=8;
					
				}
				return list2.iterator();
			}
//		}).saveAsTextFile("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/TestJsonToCsv");
//		}).saveAsTextFile("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/TestJsonToCsv2");
		}).saveAsTextFile(outputPath);
		
	}
}
