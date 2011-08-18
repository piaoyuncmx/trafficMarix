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
		
		
		
//		DBCollection col = db.getCollection("panabit_20110817");
//		DBCursor ite = col.find(new BasicDBObject("starttime", 1.3135914E9));
//		System.out.println(ite.count());
//		ite = col.find(new BasicDBObject("traffic.0.time", 1.3135914E9));
//		System.out.println(ite.count());
//		ite = col.find();
//		System.out.println(ite.count());
//		System.out.println(ite.next());
//		System.out.println(ite.next());
		
//******************The first MapReduce,to obtain useful data***************
//		String mapFunc = "function(){emit({srcgroup:this.srcgroup, dstgroup:this.dstgroup, app:this.app}, " +
//				"{traffic:{inbyte:this.traffic[0].inByte, outbyte:this.traffic[0].outByte, conn:1}})}";
//		String reduceFunc = "function(key, vals){var n = {inbyte:0, outbyte:0, conn:0};" +
//				"for (var i in vals){" +
//				"n.inbyte += vals[i].traffic.inbyte;" +
//				"n.outbyte += vals[i].traffic.outbyte;" +
//				"n.conn += vals[i].traffic.conn;}" +
//				"return {\"traffic\":n}; }";
//		DBObject query = new BasicDBObject();
//		query.put("traffic.0.time", 1.3135914E9);
//		long startTime=System.currentTimeMillis();
//		MapReduceOutput out = col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-1", query);
//		long endTime=System.currentTimeMillis(); 
//		int count = 0;
//	    for ( DBObject obj : out.results() ) {
//	         System.out.println( obj );
//	         count++;
//	     }
//	    System.out.println( (endTime-startTime)/1000+"s" );
//	    System.out.println( count );
//*********************************************************************************
		
		
		DBCollection col = db.getCollection("TrafficMatrix-1");
//		DBCursor ite = col.find(new BasicDBObject("srcgroup", "dorm"));
//		System.out.println(ite.count());
	    
//***********************transform srcGroup,dstGroup *****************************   
//	    col.updateMulti(new BasicDBObject("_id.srcgroup", new BasicDBObject("$lte", 2)), 
//				new BasicDBObject("$set", new BasicDBObject("srcgroup", "admin")));
//		col.updateMulti(new BasicDBObject("_id.srcgroup", new BasicDBObject("$lte", 6).append("$gt", 2)), 
//				new BasicDBObject("$set", new BasicDBObject("srcgroup", "office")));
//		col.updateMulti(new BasicDBObject("_id.srcgroup", new BasicDBObject("$lte", 10).append("$gt", 6)), 
//				new BasicDBObject("$set", new BasicDBObject("srcgroup", "dorm")));
//		col.updateMulti(new BasicDBObject("_id.srcgroup", 11), 
//				new BasicDBObject("$set", new BasicDBObject("srcgroup", "wireless")));
//		col.updateMulti(new BasicDBObject("_id.srcgroup", new BasicDBObject("$lte", 257).append("$gt", 11)), 
//				new BasicDBObject("$set", new BasicDBObject("srcgroup", "cernet")));
//		col.updateMulti(new BasicDBObject("_id.srcgroup", 258), 
//				new BasicDBObject("$set", new BasicDBObject("srcgroup", "unicom")));
//		col.updateMulti(new BasicDBObject("_id.srcgroup", 259), 
//				new BasicDBObject("$set", new BasicDBObject("srcgroup", "telecom")));
//		
//	    col.updateMulti(new BasicDBObject("_id.dstgroup", new BasicDBObject("$lte", 2)), 
//				new BasicDBObject("$set", new BasicDBObject("dstgroup", "admin")));
//		col.updateMulti(new BasicDBObject("_id.dstgroup", new BasicDBObject("$lte", 6).append("$gt", 2)), 
//				new BasicDBObject("$set", new BasicDBObject("dstgroup", "office")));
//		col.updateMulti(new BasicDBObject("_id.dstgroup", new BasicDBObject("$lte", 10).append("$gt", 6)), 
//				new BasicDBObject("$set", new BasicDBObject("dstgroup", "dorm")));
//		col.updateMulti(new BasicDBObject("_id.dstgroup", 11), 
//				new BasicDBObject("$set", new BasicDBObject("dstgroup", "wireless")));
//		col.updateMulti(new BasicDBObject("_id.dstgroup", new BasicDBObject("$lte", 257).append("$gt", 11)), 
//				new BasicDBObject("$set", new BasicDBObject("dstgroup", "cernet")));
//		col.updateMulti(new BasicDBObject("_id.dstgroup", 258), 
//				new BasicDBObject("$set", new BasicDBObject("dstgroup", "unicom")));
//		col.updateMulti(new BasicDBObject("_id.dstgroup", 259), 
//				new BasicDBObject("$set", new BasicDBObject("dstgroup", "telecom")));
//
//		DBCursor ite = col.find();
//		while (ite.hasNext()){
//			System.out.println(ite.next());
//		}
//******************************************************************************************		
		
		
//*************************the second MapRduce,to obtain 7 IPGroup to 7 IPGroup************		
		String mapFunc = "function(){emit({srcgroup:this.srcgroup, dstgroup:this.dstgroup, app:this._id.app}, " +
				"{traffic:{inbyte:this.value.traffic.inbyte, outbyte:this.value.traffic.outbyte, " +
				"totalbyte:this.value.traffic.inbyte+this.value.traffic.outbyte, conn:this.value.traffic.conn}})}";
		String reduceFunc = "function(key, vals){var n = {inbyte:0, outbyte:0, totalbyte:0, conn:0};" +
				"for (var i in vals){" +
				"n.inbyte += vals[i].traffic.inbyte;" +
				"n.outbyte += vals[i].traffic.outbyte;}" +
				"n.conn += vals[i].traffic.conn;}" +
				"n.totalbyte = n.inbyte + n.outbyte;" +
				"return {\"traffic\":n}; }";
		DBObject query = new BasicDBObject();
		query.put("_id.srcgroup", new BasicDBObject("$lte", 260));
		query.put("_id.dstgroup", new BasicDBObject("$lte", 260));
		long startTime=System.currentTimeMillis();
		MapReduceOutput out = col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-1-1", query);
		long endTime=System.currentTimeMillis(); 
		int count = 0;
		for ( DBObject obj : out.results() ) {
			System.out.println( obj );
			count++;
		}
		System.out.println( (endTime-startTime)+"ms" );
		System.out.println( count );
	}

}
