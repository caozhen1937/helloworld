#使用说明

###将挂载到本地的OSS文件上传到HDFS上，文件过多，用此小工具实现。
1、并且会有三个日志文件输出：成功传输的文件列表、传输失败的文件列表、传输失败的错误信息
2、如果程序崩溃，重启后会从上次做完的任务处开始，继续任务。而不是重新开始。

UploadFile.java  为main方法

###任务命令：
运行jar需要两个参数：
1、列表文件的路径inputFile
	文件列表的格式：inputPath outputPath  （中间使用空格隔开）
2、日志输出目录logDir
	例如：/user/caozhen/   （最后要有/）
3、在日志输出目录下手动创建pos.log文件
	pos.log文件记录输入文件读取的位置，初始值为0（文件的行数的偏移量是从0开始的）
	echo 0 > pos.log  

例：java -jar OSS.jar /home/easemob/caozhen/input.log /home/easemob/caozhen/













