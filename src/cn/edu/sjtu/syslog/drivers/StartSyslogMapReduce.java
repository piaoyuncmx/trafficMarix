/**
 * 
 */
package cn.edu.sjtu.syslog.drivers;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;

import cn.edu.sjtu.syslog.mapreduce.SyslogMapReduce;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * @author jianwen
 * Start MapReduce Syslog here
 */
public class StartSyslogMapReduce {

	/**
	 * @param args
	 * @throws MongoException 
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException,
			MongoException, IOException, SocketException {
		// TODO Auto-generated method stub

		String mongoIp = "localhost"; // MongoDB IP Address
		int mongoPort = 27017; // MongoDB Database Port
		String dbName = "dbpanabit"; // MongoDB Collection Name
		String collectName = "traiffcSyslog"; // MongoDB Collection Name
		double time = -1;	// current time in UTC seconds, the initial value is negative 
		
		/*
		 * Get command line params
		 */
		switch(args.length) {
		case 5:
			time = Long.parseLong(args[4]) / 600 * 600 - 600;	// the 5th param (opitional) is UTC time
		case 4:
			mongoIp = args[0];
			mongoPort = Integer.parseInt(args[1]); // MongoDB Port
			dbName = args[2]; // MongoDB Database Name
			collectName = args[3]; // MongoDB Collection
			if (time < 0) {
				time = ((long)System.currentTimeMillis()) / 1000 / 600 * 600 - 600;
			}
			break;
		default:
			System.out.println("Incorrect number of param.");
			System.out.println("Usage: java -classpath ./bin:./lib/mongo-2.6.3.jar cn.edu.sjtu.syslog.drivers.StartSyslogMapReduce <MongoIP> <MongoPort> <DBName> <CollectName> [UTCSeconds]");
			System.exit(-1);
		}


		try {
			/* Connect to MogonDB Database, Collection */
			Mongo mongo = new Mongo(mongoIp, mongoPort);
			DB dbPanabit = mongo.getDB(dbName);
			DBCollection mongoCollection = dbPanabit.getCollection(collectName);
			
			// TODO: Remove the debug output
			System.out.println("Processing traffic starting at UTCSecond: " + time);
			
			/* Call MapReduce Functions */
			SyslogMapReduce.syslogMapReduce(dbPanabit, mongoCollection, time);
			
		} catch (MongoException e) {
			e.printStackTrace();
		}
		
		
	}

}
