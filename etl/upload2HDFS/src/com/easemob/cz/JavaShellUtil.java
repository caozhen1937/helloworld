package com.easemob.cz;
 
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
 
//http://kongcodecenter.iteye.com/blog/1231177
//�ο�http://siye1982.iteye.com/blog/592405
//�ο�http://blog.csdn.net/christophe2008/article/details/6046456
public class JavaShellUtil {
    // ����·��
    private static final String basePath = "/root/";
 
    // ��¼Shellִ��״������־�ļ���λ��(����·��)
    private static final String executeShellLogFile = basePath + "executeShell.log";
 
    // �����ļ���Kondorϵͳ��Shell���ļ���(����·��)
    private static final String sendKondorShellName = basePath + "songjy.sh";
 
    public int executeShell(String shellCommand) throws IOException {
        System.out.println("shellCommand:"+shellCommand);
        int success = 0;
        StringBuffer stringBuffer = new StringBuffer();
        BufferedReader bufferedReader = null;
        // ��ʽ������ʱ�䣬��¼��־ʱʹ��
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS ");
 
        try {
            stringBuffer.append(dateFormat.format(new Date()))
                    .append("׼��ִ��Shell���� ").append(shellCommand)
                    .append(" \r\n");
            Process pid = null;
            String[] cmd = { "/bin/sh", "-c", shellCommand };
            // ִ��Shell����
            pid = Runtime.getRuntime().exec(cmd);
            if (pid != null) {
//                stringBuffer.append("���̺ţ�").append(pid.toString()).append("\r\n");
                // bufferedReader���ڶ�ȡShell���������
                bufferedReader = new BufferedReader(new InputStreamReader(pid.getInputStream()), 1024);
                pid.waitFor();
            } 
//            else {
//                stringBuffer.append("û��pid\r\n");
//            }
            stringBuffer.append(dateFormat.format(new Date())).append("Shell����ִ�����\r\nִ�н��Ϊ��\r\n");
            String line = null;
            // ��ȡShell��������ݣ�����ӵ�stringBuffer��
            while (bufferedReader != null && (line = bufferedReader.readLine()) != null) {
                stringBuffer.append(line).append("\r\n");
            }
            System.out.println("stringBuffer:"+stringBuffer);
        } catch (Exception ioe) {
            stringBuffer.append("ִ��Shell����ʱ�����쳣��\r\n").append(ioe.getMessage()).append("\r\n");
        } finally {
            if (bufferedReader != null) {
                OutputStreamWriter outputStreamWriter = null;
                try {
                    bufferedReader.close();
                    // ��Shell��ִ������������־�ļ���
                    OutputStream outputStream = new FileOutputStream(executeShellLogFile);
                    outputStreamWriter = new OutputStreamWriter(outputStream, "UTF-8");
                    outputStreamWriter.write(stringBuffer.toString());
                    System.out.println("stringBuffer.toString():"+stringBuffer.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    outputStreamWriter.close();
                }
            }
            success = 1;
        }
        return success;
    }
 
//    public static void main(String[] args) {
//        try {
//            new JavaShellUtil().executeShell(sendKondorShellName);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
}