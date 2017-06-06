package com.easemob.caozhen;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class Test1 {
	public static void main(String[] args) throws IOException {
		String str = "{\"00000000782af37f\":{\"deletedAt\":-9223372036854776000,\"columns\":[[\"\\u0000\\u0002·56\\u0000\\u0000\\nhuochebang\\u0000\\u0000\\u0012227579538123524088\\u0000\\u0000\\u0000\\u0000\",\"\",1470552311607000],[\"\\u0000\\u000256\\u0000\\u0000\\nhuochebang\\u0000\\u0000\\u0012227579538123524088\\u0000\\u0000\\u0007msgbody\\u0000\",\"能介绍一个么\",1470552311607000],[\"\\u0000\\u000256\\u0000\\u0000\\nhuochebang\\u0000\\u0000\\u0012227579538123524088\\u0000\\u0000\\u0007msgtype\\u0000\",\"txt\",1470552311607000],[\"\\u0000\\u000256\\u0000\\u0000\\nhuochebang\\u0000\\u0000\\u0012227579538123524088\\u0000\\u0000\\u0007to_user\\u0000\",\"customer_service\",1470552311607000],[\"\\u0000\\u000256\\u0000\\u0000\\nhuochebang\\u0000\\u0000\\u0012227579538123524088\\u0000\\u0000\\bchattype\\u0000\",\"chat\",1470552311607000],[\"\\u0000\\u000256\\u0000\\u0000\\nhuochebang\\u0000\\u0000\\u0012227579538123524088\\u0000\\u0000\\tdirection\\u0000\",\"outgoing\",1470552311607000],[\"\\u0000\\u000256\\u0000\\u0000\\nhuochebang\\u0000\\u0000\\u0012227579538123524088\\u0000\\u0000\\tfrom_user\\u0000\",\"a83ff3ad37e0883ef759918f26fefe5e\",1470552311607000],[\"\\u0000\\u000256\\u0000\\u0000\\nhuochebang\\u0000\\u0000\\u0012227579538123524088\\u0000\\u0000\\fmsgtimestamp\\u0000\",\"0000015663bf343f\",1470552311607000],[\"\\u0000\\u000277\\u0000\\u0000\\u0002im\\u0000\\u0000\\u0012227555939434103196\\u0000\\u0000\\u0000\\u0000\",\"\",1470546818230000],[\"\\u0000\\u000277\\u0000\\u0000\\u0002im\\u0000\\u0000\\u0012227555939434103196\\u0000\\u0000\\u0007msgbody\\u0000\",\"{\\\"from\\\":\\\"101619474\\\",\\\"to\\\":\\\"101601335\\\",\\\"type\\\":\\\"1\\\",\\\"message\\\":\\\"可是我没有什么不舒服的，就是同房时不太舒服\\\",\\\"name\\\":\\\"177****5336\\\",\\\"userType\\\":\\\"2\\\",\\\"avatar\\\":\\\"\\\",\\\"phone\\\":\\\"17715095336\\\"}\",1470546818230000],[\"\\u0000\\u000277\\u0000\\u0000\\u0002im\\u0000\\u0000\\u0012227555939434103196\\u0000\\u0000\\u0007msgtype\\u0000\",\"txt\",1470546818230000],[\"\\u0000\\u000277\\u0000\\u0000\\u0002im\\u0000\\u0000\\u0012227555939434103196\\u0000\\u0000\\u0007to_user\\u0000\",\"101601335\",1470546818230000],[\"\\u0000\\u000277\\u0000\\u0000\\u0002im\\u0000\\u0000\\u0012227555939434103196\\u0000\\u0000\\bchattype\\u0000\",\"chat\",1470546818230000],[\"\\u0000\\u000277\\u0000\\u0000\\u0002im\\u0000\\u0000\\u0012227555939434103196\\u0000\\u0000\\tdirection\\u0000\",\"outgoing\",1470546818230000],[\"\\u0000\\u000277\\u0000\\u0000\\u0002im\\u0000\\u0000\\u0012227555939434103196\\u0000\\u0000\\fmsgtimestamp\\u0000\",\"101619474\",1470546818230000],[\"\\u0000\\u000277\\u0000\\u0000\\u0002im\\u0000\\u0000\\u0012227555939434103196\\u0000\\u0000\\tfrom_user\\u0000\",\"101619474\",1470546818230000]]}}";
		
		JSONObject jsonObject = JSONObject.fromObject(str);
		JSONObject jsonObject2 = jsonObject.getJSONObject("00000000782af37f");
		JSONArray jsonArray = jsonObject2.getJSONArray("columns");
		
		List<JSONArray> list1 = new ArrayList<JSONArray>();
		for(int i=0; i<jsonArray.size();i++){
			list1.add(jsonArray.getJSONArray(i));
		}
		
		int i = 0;
		int count = 8;
		FileOutputStream outputStream = new FileOutputStream("/fromJson.csv", true);
		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
		
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
			
			Set<Entry<String,String>> entrySet = map.entrySet();
			String newline = "";
			for (Entry<String, String> entry : entrySet) {
				newline = newline + entry.getValue() + "," ;
			}
			
			outputStreamWriter.write(newline.substring(0,newline.length()-1)+"\n");
			outputStreamWriter.flush();
			System.out.println(newline.substring(0,newline.length()-1));
			i=count;
			count+=8;
			
		}
		
	}
}
