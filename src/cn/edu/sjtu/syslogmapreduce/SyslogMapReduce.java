package cn.edu.sjtu.syslogmapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.bson.types.Code;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

public class SyslogMapReduce {
	
	public void PreTreatment(){
		String code = "function PreTreatment(col){" +
				"}";
	}
	
	
	public void FirstMapReduce(DBCollection col, double time){
		/*first mapreduce */		

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
		query.put("starttime", new BasicDBObject("$gte", time).append("$lt", time+600));
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
	
	
	public void AddKeyValues(DBCollection col, double time){
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

		DBCursor ite = col.find();
		while (ite.hasNext()){
			ite.next().put("time", time);
		}
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
	
	
	public void ThirdMapReduce(DBCollection col){
		/*third mapreduce to caculate the percentage and form the last collection*/
		
		//String UTCTime = String.valueOf(time);
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
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix", MapReduceCommand.OutputType.MERGE, query);
	}
	
	
	public void FormTheFinalCollection(DB db, DBCollection col){
		String code = "function func(n){" +
				"}";
		DBCollection args = col;
		db.doEval(code, args);
	}
	
	
	public static void main(String[] args) throws UnknownHostException, MongoException{
//		BufferedReader readin = new BufferedReader(new InputStreamReader(System.in));
//		try {
//			String[] read = readin.readLine().split(" ");
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
		
		double curTime = System.currentTimeMillis()/1000;
		double time = 1.3135914E9+Math.floor((curTime-1.3135914E9)/600-1)*600;
//		System.out.println(time);
		
//		String ipAddr = "10.50.15.201";
//		String dbName = "dbpanabit";
		
		String ipAddr = args[0];
		String dbName = args[1];
		
		Mongo m = new Mongo(ipAddr);
		for (String s : m.getDatabaseNames()){
			System.out.println("DBName: "+s);
		}
		DB db = m.getDB(dbName);
		for (String s : db.getCollectionNames()){
			System.out.println("CollectionName: "+s);
		}
			
		DBCollection col = db.getCollection("trafficSyslog");
//		DBCursor ite = col.find();
//		System.out.println(ite.next());
//		System.out.println(col.find(new BasicDBObject("starttime", new BasicDBObject("$gte", time).append("$lt", time+600))).count());
		
		long startTime = System.currentTimeMillis();
		
//		SyslogMapReduce smr = new SyslogMapReduce();
//		
//		smr.FirstMapReduce(col, 1.3138542E9);
//		
//		col = db.getCollection("TrafficMatrix-1");
//		smr.AddKeyValues(col, time);
//		smr.SecondMapReduce(col);
//		
//		col = db.getCollection("TrafficMatrix-1-1");
//		smr.AllTotalbyteCaculation(col);
//		DBCollection col2 = db.getCollection("TrafficMatrix-1-1-1");
//		smr.AddAllTotalbyte(col, col2);
//		
//		smr.ThirdMapReduce(col);
		
//		col = db.getCollection("TrafficMatrix-1");
//		col.drop();
//		col = db.getCollection("TrafficMatrix-1-1");
//		col.drop();
//		col = db.getCollection("TrafficMatrix-1-1-1");
//		col.drop();
		
		long endTime = System.currentTimeMillis();
		System.out.println((endTime - startTime)/1000 + "s");
	
		
		
		
		/*for test*/
//		DBCollection col = db.getCollection("TrafficMatrix");
//		DBCursor ite = col.find();
//		DBObject temp = ite.next();
//		System.out.println(temp);
//		String cde ="function func(n){" +
//				"return n.traffic[0].time;}";
//		System.out.println(db.doEval(cde, temp));

//		while (ite.hasNext()){
//			System.out.println(ite.next());
//		}

	}
}
