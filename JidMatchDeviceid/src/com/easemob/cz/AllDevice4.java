package com.easemob.cz;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.easemob.cz.udf.FindEasemodIdUDF;
import com.easemob.cz.udf.FindOrgUDF;
import com.easemob.cz.udf.GetAppFromDeviceUDF;
import com.easemob.cz.udf.GetToken;
import com.easemob.cz.udf.ParseTimesUDF;

import scala.Tuple2;

public class AllDevice4 {

	public static void main(String[] args) {

		// SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("sparkSQL");
		SparkConf sparkConf = new SparkConf().setMaster("mesos://cdp-bj1-mesos-slave3:5050").setAppName("sparkSQL");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SparkSession sparkSession = SparkSession.builder().config(sparkConf)
				.config("spark.shuffle.sort.bypassMergeThreshold", 2000)
				.config("spark.shuffle.memoryFraction", 0.3)
				.getOrCreate();

		String deviceInpath = args[0];
		String userPath = args[1];
		String postPath = args[2];
		String deviceOutpath = args[3];
		String joinOutpath = args[4];

		// JavaRDD<String> javaRDD1 =
		// sc.textFile("hdfs://cdp-bj1-hdfs-server1:9000/sources/im/taikang_device/tk_device.log",
		// 1);
		// JavaRDD<String> javaRDD1 = sc.textFile("device.txt", 1);
		JavaRDD<String> javaRDD1 = sc.textFile(deviceInpath, 1);

		// 3种日志生成原始表
		saveDeviceInfoWithCsv(javaRDD1, sparkSession, deviceOutpath);
		saveUsergridAccessWithCsv(sparkSession, userPath);
		savePostAccessWithCsv(sparkSession, postPath);

		// 注册自定义函数
		sparkSession.udf().register("parse_times", new ParseTimesUDF(), DataTypes.StringType);
		sparkSession.udf().register("get_org", new FindOrgUDF(), DataTypes.StringType);
		sparkSession.udf().register("get_easemobid", new FindEasemodIdUDF(), DataTypes.StringType);
		sparkSession.udf().register("get_org_formD", new GetAppFromDeviceUDF(), DataTypes.StringType);
		sparkSession.udf().register("getToken", new GetToken(), DataTypes.StringType);

		joinAndSaveWithCsv(sparkSession, joinOutpath);

	}

	private static void joinAndSaveWithCsv(SparkSession sparkSession, String joinOutpath) {

		String joinSql = "SELECT get_easemobid(c.eid) easemobID,tmp.* FROM "
				+ "(SELECT  b.ip ip, b.ts time, b.org orgname,a.* FROM "
				+ "(SELECT *, get_org_formD(appkey) org FROM device ) a " +
				// "(SELECT *, get_org_formD(appkey) org FROM device WHERE
				// appkey LIKE '%/%' ) a " +
				"JOIN "
				+ "(SELECT ip , parse_times(ts) ts , get_org(orgInfo) org FROM user WHERE orgInfo LIKE 'POST /%/%/devices %/%')  b "
				+ "ON a.ts = b.ts AND a.org = b.org " + ") tmp JOIN "
				+ "(SELECT _c0 ip,parse_times(_c2) ts, get_org(_c3) org, getToken(_c8,_c9,_c10) eid  FROM post WHERE _c3 LIKE 'POST /%/%/% %/%') c "
				+ " ON tmp.ip = c.ip AND tmp.time = c.ts  AND  tmp.orgname = c.org ";

		Dataset<Row> df = sparkSession.sql(joinSql);
		Dataset<Row> redf = df.repartition(1);
		// redf.write().format("com.databricks.spark.csv").option("header", true).save("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output888");
		redf.write().format("com.databricks.spark.csv").option("header", true).save(joinOutpath);

	}

	private static void savePostAccessWithCsv(SparkSession sparkSession, String postPath) {

		// ArrayList<StructField> structFields = new ArrayList<StructField>();
		// String[] item = new
		// String[]{"ip","toIP","ts","orgInfo","str1","str2","str3",
		// "str4","str5","str6","easemobid"};
		// for(int i=0;i<item.length;i++){
		// structFields.add(DataTypes.createStructField(item[i],
		// DataTypes.StringType, true));
		// }
		// StructType postschema = DataTypes.createStructType(structFields);

		Dataset<Row> df = sparkSession.read()
				.format("com.databricks.spark.csv")
				.option("inferSchema", "true")
				// .schema(postschema)
				.option("delimiter", " ")
				// .load("hdfs://cdp-bj1-hdfs-server1:9000/sources/im/taikang_device/web*post-token-access*");
				// .load("post.txt");
				.load(postPath);
		df.createOrReplaceTempView("post");
		df.show();
		// df.write().format("com.databricks.spark.csv").option("header",
		// true).save("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output555");
	}

	private static void saveUsergridAccessWithCsv(SparkSession sparkSession, String userPath) {

		ArrayList<StructField> structFields = new ArrayList<StructField>();
		String[] item = new String[] { "ip", "toIP", "str1", "ts", "orgInfo", "str2", "str3", "str4", "str5", "str6",
				"str7", "str8", "str9", "str10" };
		for (int i = 0; i < item.length; i++) {
			structFields.add(DataTypes.createStructField(item[i], DataTypes.StringType, true));
		}
		StructType userschema = DataTypes.createStructType(structFields);

		Dataset<Row> df = sparkSession.read()
				.format("com.databricks.spark.csv")
				.schema(userschema)
				.option("delimiter", " ")
				.load(userPath);
		df.createOrReplaceTempView("user");
		df.show();

	}

	private static void saveDeviceInfoWithCsv(JavaRDD<String> javaRDD1, SparkSession sparkSession,
			String deviceOutpath) {

		JavaRDD<Row> rowRDD = javaRDD1.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;

			Map<String, String> allInfo = new HashMap<String, String>();

			@Override
			public Row call(String line) throws Exception {
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
				String uuid = allInfo.get("uuid");
				String type = allInfo.get("type");
				String created = allInfo.get("created");
				String modified = allInfo.get("modified");
				String token = allInfo.get("token");
				String model = allInfo.get("model");
				String manufacturer = allInfo.get("manufacturer");
				String appKey = allInfo.get("appKey");
				String version = allInfo.get("version");

				String imei = "null";
				if (allInfo.get("imei") != null) {
					imei = allInfo.get("imei");
				}
				String loc_lat = "null";
				if (allInfo.get("loc.lat") != null) {
					loc_lat = allInfo.get("loc.lat");
				}
				String loc_lng = "null";
				if (allInfo.get("loc.lng") != null) {
					loc_lng = allInfo.get("loc.lng");
				}
				String operator = "null";
				if (allInfo.get("operator") != null) {
					operator = allInfo.get("operator");
				}

				long ts1_1 = Long.valueOf(created).longValue() / 1000;
				String ts1_2 = Long.valueOf(ts1_1).toString();

				long ts5_1 = ts1_1 / 5; // 时间精确度5s
				String ts5_2 = Long.valueOf(ts5_1).toString();

				return RowFactory.create(ts1_2, uuid, type, created, modified, token, model, manufacturer, appKey,
						version, imei, loc_lat, loc_lng, operator);
			}
		});

		ArrayList<StructField> structFields = new ArrayList<StructField>();
		String[] item = new String[] { "ts", "uuid", "type", "created", "modified", "token", "model", "manufacturer",
				"appKey", "version", "imei", "loc.lat", "loc.lng", "operator" };
		for (int i = 0; i < item.length; i++) {
			structFields.add(DataTypes.createStructField(item[i], DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(structFields);
		Dataset<Row> df = sparkSession.createDataFrame(rowRDD, schema);

		df.createOrReplaceTempView("device");
		df.show();
		// df.write().csv("hdfs://cdp-bj1-hdfs-server1:9000/user/caozhen/output30");
		// df.write().csv(deviceOutpath);

	}
}
