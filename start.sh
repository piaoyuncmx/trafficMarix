#!/bin/sh
java -classpath ./bin:./lib/mongo-2.6.3.jar cn.edu.sjtu.syslogmapreduce.SyslogMapReduce localhost 27017 dbpanabit trafficSyslog
