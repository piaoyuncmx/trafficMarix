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

		/*
		 * Get command line params
		 */
		if (args.length != 4) {
			System.out.println("USAGE: java");
		} else {
			mongoIp = args[0];
			mongoPort = Integer.parseInt(args[1]); // MongoDB Port
			dbName = args[2]; // MongoDB Database Name
			collectName = args[3]; // MongoDB Collection
		}

		try {
			/* Get current time, UTC, in seconds */
			// TODO: correct the time
//			int time = (int) System.currentTimeMillis() / 1000;
			double curTime = System.currentTimeMillis()/1000;
			double time = 1.3135914E9+Math.floor((curTime-1.3135914E9)/600-1)*600;

			/* Connect to MogonDB Database, Collection */
			Mongo mongo = new Mongo(mongoIp, mongoPort);
			DB dbPanabit = mongo.getDB(dbName);
			DBCollection mongoCollection = dbPanabit.getCollection(collectName);
			
			/* Call MapReduce Functions */
			SyslogMapReduce.syslogMapReduce(dbpanabit, mongoCollection, time);
			
		} catch (MongoException e) {
			e.printStackTrace();
		}
		
		
	}

}
