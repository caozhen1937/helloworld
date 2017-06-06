package com.easemob.cz;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class Test2 {
	
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		String logDir = args[1];
		
		List<String> jobList = getJobList(inputFile);
		
		int currentPos = getErrPos(logDir);
		
		for (String job : jobList) {
			
			try{
				String[] split = job.split(" ");
				Process process = uploadToHDFS(split[0], split[1]);
			    
				logFail(job,logDir,process);
				logPos(currentPos + 1, logDir);
				
				logSuccess(job, logDir);
				currentPos += 1;
			}catch (Exception e){
				e.printStackTrace();
			}
			
		}
		
		
//		for(job: jobList[currentPos, jobList.length()-1]){
//		}
	}
	
	
	public static Process uploadToHDFS(String localFile,String HDFSFile) throws InterruptedException{
		Process process = null;
		String command = "hadoop fs -put " + localFile + " " + HDFSFile;
		String line = null;
		try {
			InputStreamReader stdISR = null;
			InputStreamReader errISR = null;
			process = Runtime.getRuntime().exec(command);
			
			stdISR = new InputStreamReader(process.getInputStream());
			BufferedReader stdBR = new BufferedReader(stdISR);
			while ((line = stdBR.readLine()) != null) {
				System.out.println("STD line:" + line);
			}
			
//			process.waitFor();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return process;
	}
	
	public static void logSuccess(String job,String logDir) throws IOException{
		FileOutputStream outputStream = new FileOutputStream(logDir + "stadata.log",true);
		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
		outputStreamWriter.write(job);
		outputStreamWriter.flush();
	}
	
	public static void logFail(String job,String logDir,Process process) throws IOException{
		FileOutputStream outputStream1 = new FileOutputStream(logDir + "errdata.log",true);
		FileOutputStream outputStream2 = new FileOutputStream(logDir + "errinfo.log",true);
		OutputStreamWriter outputStreamWriter1 = new OutputStreamWriter(outputStream1);
		OutputStreamWriter outputStreamWriter2 = new OutputStreamWriter(outputStream2);
		
		InputStreamReader errISR = new InputStreamReader(process.getErrorStream());
		BufferedReader errBR = new BufferedReader(errISR);
		
		String line = null;
		while ((line = errBR.readLine()) != null) {
			System.out.println("ERR line:" + line);
			
			outputStreamWriter1.write(job + "\n");
			outputStreamWriter1.flush();
			
			outputStreamWriter2.write(line);
			outputStreamWriter2.flush();
		}
	}

	public static void logPos(int pos, String logDir) throws IOException{
		File posFile = new File(logDir + "/pos.log");
		OutputStreamWriter outputStreamWriter4 = new OutputStreamWriter(
				new FileOutputStream(posFile));
		
		outputStreamWriter4.write(pos);	
		outputStreamWriter4.flush();
	}
	public static int getErrPos(String logDir) throws Exception{
		File posFile = new File(logDir + "/pos.log");
		FileInputStream fis1 = new FileInputStream(posFile);
		InputStreamReader isr1 = new InputStreamReader(fis1);
		BufferedReader br1 = new BufferedReader(isr1);
		String pos = "";
		if((pos = br1.readLine())== null){
			return 0;
		}else{	
			return Integer.valueOf(pos);
		}
	}

	public static List<String> getJobList(String inputFile) throws IOException{
		
		FileInputStream fis = new FileInputStream(inputFile);
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);
		
		List<String> jobList = new ArrayList<>();
		String line = null;
		
		while((line=br.readLine())!=null){
			jobList.add(line);
		}
		return jobList;
	}
	
}
