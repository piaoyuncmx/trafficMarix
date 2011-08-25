@echo off
java -classpath ./bin;./lib/mongo-2.6.3.jar cn.edu.sjtu.syslog.drivers.StartSyslogMapReduce localhost 27017 dbpanabit trafficSyslog
pause
