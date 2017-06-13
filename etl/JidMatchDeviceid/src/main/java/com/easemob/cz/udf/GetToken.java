package com.easemob.cz.udf;

import org.apache.spark.sql.api.java.UDF3;

public class GetToken implements UDF3<String, String, String, String>{
	private static final long serialVersionUID = 1L;

	public String call(String str1, String str2, String str3) throws Exception {
		if(str1.contains("||")){
			return str2;
		}else if(str2.contains("||")){
			return str3;
		}else{
			return str3;
		}
	}

}
