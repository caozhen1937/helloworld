package com.easemob.cz.udf;

import org.apache.spark.sql.api.java.UDF1;

public class GetAppFromDeviceUDF implements UDF1<String, String>{

	private static final long serialVersionUID = 1L;

	@Override
	public String call(String line) throws Exception {
		String[] split = line.split("/");
		
		return split[0];
	}

}
