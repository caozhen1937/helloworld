package com.easemob.cz;

public class Test3 {
		//	uploadFile(jobList,logDir);
		//	
		//	jobList = getJobList(inputFile)
		//	void uploadFile(jobList, logDir){
		//		currentPos = getErrPos(logDir)
		//		for(job: jobList[currentPos, jobList.length()]){
		//			try{
		//			    uploadToHDFS(job[0], job[1]))
		//			}catch (e){
		//				logFail(job,logDir);
		//				logPos(currentPos + 1, logDir);
		//				throw e
		//			}
		//			logSuccess(job, logDir);
		//			currentPos += 1;
		//		}
		//	}
		//	List<String[]> getJobList(inputFile){
		//		lines = readFile()
		//		jobList List<String[]>{}
		//		for(line: lines){
		//			localFile, HDFSFile = " ".split(line);
		//			jobList.append(String[]{localFile, HDFSFile});
		//		}
		//		return jobList
		//	}
		//	void uploadToHDFS(localFile, HDFSFile){
		//	
		//	}
		//	}
		//	int getErrPos(logDir){
		//		posFile = logDir + "/pos.log"
		//		if(not exsit posFile){
		//			return 0
		//		}else{	
		//			return (int) File.read(posFile)
		//		}
		//	}
		//	void logPos(pos, logDir){
		//		posFile = logDir + "/pos.log"
		//		f = File.open(posFile£¬"w")
		//		f.write(pos)
		//	}
	
}
