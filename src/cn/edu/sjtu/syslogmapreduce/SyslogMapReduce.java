package cn.edu.sjtu.syslogmapreduce;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

public class SyslogMapReduce {
	public static void main(String[] args) throws UnknownHostException, MongoException{
		Mongo m = new Mongo("10.50.15.205");
		for (String s : m.getDatabaseNames()){
			System.out.println("DBName: "+s);
		}
		DB db = m.getDB("dbpanabit");
		for (String s : db.getCollectionNames()){
			System.out.println("CollectionName: "+s);
		}
		DBCollection col = db.getCollection("panabit_20110817");
		DBCursor ite = col.find();
		System.out.println(ite.next());
		System.out.println(ite.next());
		System.out.println(ite.next());
//		int count = 0;
//		while (ite.hasNext()){
//			System.out.println(ite.next());
//			count++;
//		}
//		System.out.println( count );
		String mapFunc = "function(){emit({srcgroup:this.srcgroup, dstgroup:this.dstgroup, app:this.app}, " +
				"{traffic:{inbyte:this.traffic[0].inByte, outbyte:this.traffic[0].outByte, conn:1}})}";
		String reduceFunc = "function(key, vals){var n = {inbyte:0, outbyte:0, conn:0};" +
				"for (var i in vals){" +
				"n.inbyte += vals[i].traffic.inbyte;" +
				"n.outbyte += vals[i].traffic.outbyte;" +
				"n.conn += vals[i].traffic.conn;}" +
				"return {\"traffic\":n}; }";
		DBObject query = new BasicDBObject();
//		query.put("traffic[0].time", new BasicDBObject("$gte", 1.3135512E9));
//		query.put("endtime", new BasicDBObject("$lte", 1.31346417E9));
		long startTime=System.currentTimeMillis();
		MapReduceOutput out = col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-1", query);
		long endTime=System.currentTimeMillis(); 
		int count = 0;
	    for ( DBObject obj : out.results() ) {
	         System.out.println( obj );
	         count++;
	     }
	    System.out.println( (endTime-startTime)/1000 );
	    System.out.println( count );
	      
	}

}
