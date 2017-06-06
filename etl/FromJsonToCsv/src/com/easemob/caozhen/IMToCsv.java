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

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class IMToCsv {
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setMaster("mesos://cdp-bj1-mesos-slave3:5050").setAppName("toCSV");
//		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("toCSV");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> javaRDD = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/user/mayuchao/usersJson/aeg-00000");
//		JavaRDD<String> javaRDD = sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/tail1-aeg-im");
		
		javaRDD.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				String[] split = line.split("\t");
				JSONObject jsonObject = JSONObject.fromObject(split[1]);
				JSONObject jsonObject2 = jsonObject.getJSONObject(split[0]);
				JSONArray jsonArray = jsonObject2.getJSONArray("columns");
				
				List<JSONArray> list1 = new ArrayList<JSONArray>();
				List<String> list2 = new ArrayList<String>();
				for(int i=0; i<jsonArray.size();i++){
					list1.add(jsonArray.getJSONArray(i));
				}
				
				int i = 0;
				int count = 8;
				
				while(i < list1.size()){
					List<JSONArray> sublist =list1.subList(i, count); 
					Map<String,String> map = new HashMap<String,String>();
					String ts =null;
					for (int j = 0; j <sublist.size(); j++) {
						map.put(j+"_1", StringEscapeUtils.escapeJavaScript(sublist.get(j).getString(0)));
						map.put(j+"_2", sublist.get(j).getString(1).replace(",", " "));
						ts = sublist.get(j).getString(2);
					}
					map.put("time", ts);
					
					String newline = "";
					for(int k =0;k < sublist.size();k++){
						newline = newline + map.get(k +"_1")+ ","+ map.get(k +"_2") + ",";
					}
					newline= newline + map.get("time");
					
//					Set<Entry<String,String>> entrySet = map.entrySet();
//					for (Entry<String, String> entry : entrySet) {
//						newline = newline + entry.getValue() + "," ;
//					}
//					list2.add(newline.substring(0,newline.length()-1));
					
					list2.add(newline);
					i=count;
					count+=8;
					
				}
				return list2.iterator();
			}
		}).saveAsTextFile("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/JsonToCsvAll2");
		
	}
}
