package com.easemob.cz;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import net.sf.json.JSONObject;


public class TimeUtils {
	public static void main(String[] args) throws ParseException {

//		System.out.println(toSeconds("[2017-03-25T00:01:01+08:00]"));
//		
//		Scanner scan = new Scanner(System.in);
//		String nextLine = scan.nextLine().replace("\\x22", "\"");
////		String  requestStr="{\\x22grant_type\\x22:\\x22password\\x22,\\x22username\\x22:\\x22null\\x22,\\x22password\\x22:\\x22\\x22,\\x22timestamp\\x22:1490371257694}".replace("\\x22", "\"");  
//        JSONObject jsonObject =  JSONObject.fromObject(nextLine);   
//        System.out.println(jsonObject.getString("username"));  
		String[] item = new String[]{"ts","uuid","type","created","modified","token","model",
				"manufacturer","appKey","version","imei","loc.lat","loc.lng","operator"};
		System.out.println(item);
		
	}
	
	/*
	 * [2017-03-26T00:00:14+08:00]格式的转换为字符串
	 */
	public static Long toSeconds(String timeStr) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        timeStr = timeStr.substring(1,20).replace("T", " ");
        Date date = formatter.parse(timeStr);
        return date.getTime()/1000;
    }
	public static Long toSeconds2(String timeStr) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        timeStr = timeStr.substring(1,20).replace("T", " ");
        Date date = formatter.parse(timeStr);
        return date.getTime()/5000;
    }
//	public static Long toSeconds2(String timeStr) throws ParseException {
//        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date date = formatter.parse(timeStr);
//        return date.getTime()/1000;
//    }
	
	
}
