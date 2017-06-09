package com.easemob.caozhen;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.codec.StringEncoder;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

public class Utils {
	public static void main(String[] args) throws IOException {

//		String str = "0002353600000a68756f63686562616e6700001232323735373935333831323335323430383800000000";
//		byte[] newstr = decodeHex(str.toCharArray());
//		String line = new String(newstr,"UTF-8");
//		
//		System.out.println(StringEscapeUtils.escapeJava(line));
//		System.out.println(unicode2String(str));
		
		//16进制的时间转换为时间戳
		String ts = "0000015664522e07";
		System.out.println(Long.valueOf(ts,16).toString());
	}
	
	
//	数据示例 77 im 227555939434103196	direction
	public static String unicode2String (String str) throws UnsupportedEncodingException{
		byte[] newstr = decodeHex(str.toCharArray());
		String line = new String(newstr,"UTF-8").replace("\n", "\u0002")
				.replace("\r", "\u0002").replace("\b", "\u0002")
				.replace("\f", "\u0002");
		
		
		StringBuffer sb = new StringBuffer();
		for(int i=0; i< line.length();i++){
			
			if(line.charAt(i)=='\u0000'){
				sb.append("");
			}else if(line.charAt(i)=='\u0002'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0012'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0007'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0003'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0004'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0005'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0006'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0008'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0009'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0010'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0011'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0014'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u000F'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u000B'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u000C'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u0013'){
				sb.append(" ");
			}else if(line.charAt(i)=='\u000E'){
				sb.append(" ");
			}else{
				sb.append(line.charAt(i));
			}
		}
		return sb.toString(); 
	}
	
	public static byte[] decodeHex(char[] data) {

		int len = data.length;
		if ((len & 0x01) != 0) {
			throw new RuntimeException("Odd number of characters.");
		}
		byte[] out = new byte[len >> 1];

		// two characters form the hex value.
		for (int i = 0, j = 0; j < len; i++) {
			int f = toDigit(data[j], j) << 4;
			j++;
			f = f | toDigit(data[j], j);
			j++;
			out[i] = (byte) (f & 0xFF);
		}
		return out;
	}

	protected static int toDigit(char ch, int index) {
		int digit = Character.digit(ch, 16);
		if (digit == -1) {
			throw new RuntimeException("Illegal hexadecimal character " + ch + " at index " + index);
		}
		return digit;
	}
}
