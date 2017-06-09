package com.bjsxt.spark.text;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class MySortTo {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("sort0").setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> javaRDD = sc.textFile("secondSort.txt");
		//文件名要写全，包括扩展名
		
		JavaPairRDD<SecondSortKey, String> sortByKeyRDD = javaRDD.mapToPair(new PairFunction<String, SecondSortKey, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<SecondSortKey, String> call(String line) throws Exception {
				
				String[] split = line.split(" ");
				int first = Integer.valueOf(split[0]);
				int second = Integer.valueOf(split[1]);
				SecondSortKey secondSortKey = new SecondSortKey(first, second);
				
				return new Tuple2<SecondSortKey, String>(secondSortKey, line);
			}
		}).sortByKey(false);
		
		
		sortByKeyRDD.foreach(new VoidFunction<Tuple2<SecondSortKey,String>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<SecondSortKey, String> line) throws Exception {
				
				System.out.println(line._2);
			}
		});
		
		
		
	}
}
