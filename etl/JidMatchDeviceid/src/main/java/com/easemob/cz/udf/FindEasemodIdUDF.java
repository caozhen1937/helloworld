package com.easemob.cz.udf;

import org.apache.spark.sql.api.java.UDF1;
import net.sf.json.JSONObject;

public class FindEasemodIdUDF implements UDF1<String, String>{

	private static final long serialVersionUID = 1L;

	public String call(String line) throws Exception{
		
		String easemobId = "FAILDFIND";
		try{
			String easemobInfo = line.replace("\\x22", "\"").replace("\\x5C", "\\");
			if (easemobInfo.indexOf("username") > -1 && easemobInfo.indexOf("{") > -1) {
				JSONObject jsonObject = JSONObject.fromObject(easemobInfo);
				easemobId = jsonObject.getString("username");
			}
			return easemobId;
		}catch(Exception e ){
			return easemobId;
		}
	}
	
}
