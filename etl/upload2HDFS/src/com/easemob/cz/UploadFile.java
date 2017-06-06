package com.easemob.cz;

/*
 * * ����jar��Ҫ����������
 * 1���б��ļ���·��inputFile
 * 2����־���Ŀ¼logDir�����磺/user/caozhen/     ���Ҫ��/��
 * ����#java -jar OSS.jar /home/easemob/caozhen/input.log /home/easemob/caozhen/
 * 
 * pos.log�ļ�������¼����λ�ã���ʼֵΪ0����Ҫ�����յ��ļ����ļ���������ƫ�����Ǵ�0��ʼ�ģ�
 * #echo 0 > pos.log
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class UploadFile {
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		String logDir = args[1];

		InputStreamReader errISR = null;

		Process process = null;
		String line = null;

		//������־
		FileOutputStream outputStream1 = new FileOutputStream(logDir + "stadata.log", true);
		FileOutputStream outputStream2 = new FileOutputStream(logDir + "errdata.log", true);
		FileOutputStream outputStream3 = new FileOutputStream(logDir + "errinfo.log", true);

		OutputStreamWriter outputStreamWriter1 = new OutputStreamWriter(outputStream1);
		OutputStreamWriter outputStreamWriter2 = new OutputStreamWriter(outputStream2);
		OutputStreamWriter outputStreamWriter3 = new OutputStreamWriter(outputStream3);
		
		int currentPos = getErrPos(logDir);
		
		List<String> jobList = getJobList(inputFile);
		List<String> subList = jobList.subList(currentPos, jobList.size());
		
		for (String jobline : subList) {
			
			String inputPath = jobline.split(" ")[0];
			String outputPath = jobline.split(" ")[1];
			String command = "sudo -E env  PATH=/usr/local/bin/:/home/easemob/mayuchao/gradle/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/home/easemob/bin:/usr/local/java/jdk/bin:/opt/cassandra/bin:/opt/ejabberd/bin:/usr/local/bin:/home/easemob/bin:/data/apps/opt/hadoop/bin/:/data/apps/opt/hive/package/bin:/data/apps/opt/hbase/package/bin HADOOP_USER_NAME=easemob hadoop fs -put " + inputPath + " " + outputPath;
//			String command = "hadoop fs -put " + inputPath + " " + outputPath;

			try {
				process = Runtime.getRuntime().exec(command.toString());
				
				//�������
				errISR = new InputStreamReader(process.getErrorStream());
				BufferedReader errBR = new BufferedReader(errISR);
				while ((line = errBR.readLine()) != null) {
					System.out.println("STD line:" + line);
					
					outputStreamWriter2.write(jobline+"\n");
					outputStreamWriter2.flush();
					
					outputStreamWriter3.write(line+"\n");
					outputStreamWriter3.flush();
					
					currentPos +=1;
					logPos(currentPos, logDir);
					
					System.exit(0);
				}

				//�������
				outputStreamWriter1.write(jobline+"\n");
				outputStreamWriter1.flush();

				currentPos += 1;
				process.waitFor();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			if (errISR != null) {
				errISR.close();
			}
			if (process != null) {
				process.destroy();
			}
		} catch (IOException e) {
		}
	}

	public static void logSuccess(String job,String logDir) throws IOException{
		
	}
	public static void logFail(String job,String logDir,Process process) throws IOException{
		
	}
	
	/*
	 * ��¼����λ��
	 */
	public static void logPos(int pos, String logDir) throws IOException {
		File posFile = new File(logDir + "pos.log");
		OutputStreamWriter outputStreamWriter = new OutputStreamWriter(new FileOutputStream(posFile));

		outputStreamWriter.write(pos + "\n");
		outputStreamWriter.flush();
	}

	/*
	 * ��ȡ����λ��
	 */
	public static int getErrPos(String logDir) throws Exception {
		File posFile = new File(logDir + "pos.log");
		FileInputStream fis1 = new FileInputStream(posFile);
		InputStreamReader isr1 = new InputStreamReader(fis1);
		BufferedReader br1 = new BufferedReader(isr1);
		String pos = "0";
		
		if ((pos = br1.readLine())==null) {
			return 0;
		} else {
			return Integer.valueOf(pos);
		}
	}

	/*
	 * ��ȡҪ����� �ļ��б� 
	 */
	public static List<String> getJobList(String inputFile) throws IOException {

		FileInputStream fis = new FileInputStream(inputFile);
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);

		List<String> jobList = new ArrayList<>();
		String line = null;

		while ((line = br.readLine()) != null) {
			jobList.add(line);
		}
		return jobList;
	}
}
