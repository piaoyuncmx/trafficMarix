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
	public void FirstMapReduce(DBCollection col, double time){
		/*first mapreduce,to grasp useful information and exchange srcGroup and dstGroup */		

		String mapFunc = "function(){if (this.srcgroup <= 11) " +
				"emit({srcgroup:this.srcgroup, dstgroup:this.dstgroup, app:this.app}, " +
				"{traffic:{inbyte:this.traffic[0].inByte, outbyte:this.traffic[0].outByte, conn:1}});" +
				"else " +
				"emit({srcgroup:this.dstgroup, dstgroup:this.srcgroup, app:this.app}, " +
				"{traffic:{inbyte:this.traffic[0].outByte, outbyte:this.traffic[0].inByte, conn:1}}); }";
		String reduceFunc = "function(key, vals){var n = {inbyte:0, outbyte:0, conn:0};" +
				"for (var i in vals){" +
				"n.inbyte += vals[i].traffic.inbyte;" +
				"n.outbyte += vals[i].traffic.outbyte;" +
				"n.conn += vals[i].traffic.conn;}" +
				"return {\"traffic\":n}; }";
		DBObject query = new BasicDBObject();
		query.put("traffic.0.time", time);
//		long startTime=System.currentTimeMillis();
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-1", query);
//		long endTime=System.currentTimeMillis(); 
//		int count = 0;
//	    for ( DBObject obj : out.results() ) {
//	         System.out.println( obj );
//	         count++;
//	     }
//	    System.out.println( (endTime-startTime)/1000+"s" );
//	    System.out.println( count );
	}

	
	public void AddKeyValues(DBCollection col){
		/*add more key-values to TrafficMatrix-1*/		
  
	    col.updateMulti(new BasicDBObject("_id.srcgroup", new BasicDBObject("$lte", 2)), 
				new BasicDBObject("$set", new BasicDBObject("srcgroup", "admin")));
		col.updateMulti(new BasicDBObject("_id.srcgroup", new BasicDBObject("$lte", 6).append("$gt", 2)), 
				new BasicDBObject("$set", new BasicDBObject("srcgroup", "office")));
		col.updateMulti(new BasicDBObject("_id.srcgroup", new BasicDBObject("$lte", 10).append("$gt", 6)), 
				new BasicDBObject("$set", new BasicDBObject("srcgroup", "dorm")));
		col.updateMulti(new BasicDBObject("_id.srcgroup", 11), 
				new BasicDBObject("$set", new BasicDBObject("srcgroup", "wireless")));
	   
		col.updateMulti(new BasicDBObject("_id.dstgroup", new BasicDBObject("$lte", 257).append("$gt", 11)), 
				new BasicDBObject("$set", new BasicDBObject("dstgroup", "cernet")));
		col.updateMulti(new BasicDBObject("_id.dstgroup", 258), 
				new BasicDBObject("$set", new BasicDBObject("dstgroup", "unicom")));
		col.updateMulti(new BasicDBObject("_id.dstgroup", 259), 
				new BasicDBObject("$set", new BasicDBObject("dstgroup", "telecom")));
		col.updateMulti(new BasicDBObject("_id.dstgroup", new BasicDBObject("$gte", 260)), 
				new BasicDBObject("$set", new BasicDBObject("dstgroup", "abroad")));
			
		col.updateMulti(new BasicDBObject("_id.app", new BasicDBObject("$ne", "http").append("$ne", "bt").append("$ne", "pplive")), 
				new BasicDBObject("$set", new BasicDBObject("app", "else")));
		col.updateMulti(new BasicDBObject("_id.app", "http"), 
				new BasicDBObject("$set", new BasicDBObject("app", "http")));
		col.updateMulti(new BasicDBObject("_id.app", "bt"), 
				new BasicDBObject("$set", new BasicDBObject("app", "bt")));
		col.updateMulti(new BasicDBObject("_id.app", "pplive"), 
				new BasicDBObject("$set", new BasicDBObject("app", "pplive")));
	}
	
	
	public void SecondMapReduce(DBCollection col){
		/*second mapreduce*/		
		
		String mapFunc = "function(){emit({srcgroup:this.srcgroup, dstgroup:this.dstgroup, app:this.app}, " +
				"{traffic:{app:this.app, inbyte:this.value.traffic.inbyte, outbyte:this.value.traffic.outbyte, " +
				"totalbyte:this.value.traffic.inbyte+this.value.traffic.outbyte, percentage:0, conn:this.value.traffic.conn}})}";
		String reduceFunc = "function(key, vals){var n = {app:\"-\", inbyte:0, outbyte:0, totalbyte:0, percentage:0, conn:0};" +
				"for (var i in vals){" +
				"n.app = vals[i].traffic.app;" +
				"n.inbyte += vals[i].traffic.inbyte;" +
				"n.outbyte += vals[i].traffic.outbyte;}" +
				"n.conn += vals[i].traffic.conn;}" +
				"n.totalbyte = n.inbyte + n.outbyte;" +
				"return {\"traffic\":n}; }";
		DBObject query = new BasicDBObject();
		query.put("_id.srcgroup", new BasicDBObject("$lte", 11));
		query.put("_id.dstgroup", new BasicDBObject("$gt", 11));
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-1-1", query);

//		int count = 0;
//		for ( DBObject obj : out.results() ) {
//			System.out.println( obj );
//			count++;
//		}
//		System.out.println( count );
	}
	
	
	public void AllTotalbyteCaculation(DBCollection col){
		/*caculate the totalbyte of all kinds */
		
		String mapFunc = "function(){emit({srcgroup:this._id.srcgroup, dstgroup:this._id.dstgroup}, this.value.traffic.totalbyte)}";
		String reduceFunc = "function(key, vals){var n = 0;" +
				"for (var i in vals){" +
				"n += vals[i];}" +
				"return n;}";
		DBObject query = new BasicDBObject();
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-1-1-1", query);
	}
	
	
	public void AddAllTotalbyte(DBCollection col, DBCollection col2){
		/*add the key-value of tototalbyte to each document in TrafficMatrix-1-1*/	
//		DBCollection col = db.getCollection("TrafficMatrix-1-1");
//		DBCollection col2 = db.getCollection("TrafficMatrix-1-1-1");

		DBObject temp = col2.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "abroad")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "cernet")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "unicom")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "telecom")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "abroad")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "cernet")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "unicom")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "telecom")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "abroad")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "cernet")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "unicom")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "telecom")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "abroad")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "cernet")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "unicom")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "telecom")).next();
	    col.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("tototalbyte", temp)));
	}
	
	
	public void ThirdMapReduce(DBCollection col, double time){
		/*third mapreduce to caculate the percentage and form the last collection*/
		
		String UTCTime = String.valueOf(time);
		String mapFunc = "function(){" +
				"this.value.traffic.percentage = this.value.traffic.totalbyte/this.tototalbyte.value;" +
				"emit({srcgroup:this._id.srcgroup, dstgroup:this._id.dstgroup}, {traffic:[this.value.traffic]}) }";
		String reduceFunc = "function(key, vals){var n = {traffic:[]};" +
				"for (var i in vals){" +
				"vals[i].traffic.forEach(function(traffic){" +
				"n.traffic.push(traffic);" +
				"});" +
				"}" +
				"return n; }";
		DBObject query = new BasicDBObject();
		col.mapReduce(mapFunc, reduceFunc, UTCTime, query);
	}
	
	
	public static void main(String[] args) throws UnknownHostException, MongoException{
		double curTime = System.currentTimeMillis()/1000;
		double time = 1.3135914E9+Math.floor((curTime-1.3135914E9)/600-1)*600;
		System.out.println(time);
		
		Mongo m = new Mongo("10.50.15.205");
		for (String s : m.getDatabaseNames()){
			System.out.println("DBName: "+s);
		}
		DB db = m.getDB("dbpanabit");
		for (String s : db.getCollectionNames()){
			System.out.println("CollectionName: "+s);
		}
			
		SyslogMapReduce smr = new SyslogMapReduce();
		
		DBCollection col = db.getCollection("panabit_20110817");
		smr.FirstMapReduce(col, time);
		
		col = db.getCollection("TrafficMatrix-1");
		smr.AddKeyValues(col);
		smr.SecondMapReduce(col);
		
		col = db.getCollection("TrafficMatrix-1-1");
		smr.AllTotalbyteCaculation(col);
		DBCollection col2 = db.getCollection("TrafficMatrix-1-1-1");
		smr.AddAllTotalbyte(col, col2);
		
		smr.ThirdMapReduce(col, time);
		
		/*delete staging database*/
		col = db.getCollection("TrafficMatrix-1");
		col.drop();
		col = db.getCollection("TrafficMatrix-1-1");
		col.drop();
		col = db.getCollection("TrafficMatrix-1-1-1");
		col.drop();
		
		/*for test*/
//		DBCollection col = db.getCollection("TrafficMatrix-1-1-2");
//		DBCursor ite = col.find();
//		System.out.println(ite.next());
//		while (ite.hasNext()){
//			System.out.println(ite.next());
//		}

	}
}
