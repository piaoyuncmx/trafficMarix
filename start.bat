@echo off
java -classpath ./bin;./lib/mongo-2.6.3.jar cn.edu.sjtu.syslogmapreduce.StartSyslogMapReduce localhost 27017 dbpanabit trafficSyslog
pause
