package com.easemob.cz.udf;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.sql.api.java.UDF1;

public class ParseTimesUDF implements UDF1<String, String>{

	private static final long serialVersionUID = 1L;

	@Override
	public String call(String timeStr) throws Exception {
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        timeStr = timeStr.substring(1,20).replace("T", " ");
        Date date = formatter.parse(timeStr);
        return String.valueOf(date.getTime()/1000).toString();
	}

}
