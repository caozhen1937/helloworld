package com.easemob.cz.udf;

import org.apache.spark.sql.api.java.UDF1;

public class FindOrgUDF implements UDF1<String, String>{

	private static final long serialVersionUID = 1L;

	public String call(String line) throws Exception {
		String postInfo = line.split(" ")[1];
		String org = postInfo.split("/")[1];
		return org;
	}
	
}
