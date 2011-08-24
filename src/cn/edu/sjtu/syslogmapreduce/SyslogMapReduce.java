package cn.edu.sjtu.syslogmapreduce;

<<<<<<< HEAD
=======
<<<<<<< HEAD
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
=======
>>>>>>> 52e9630e409f73a76cb0be7f8fc13126aca5e2ba
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
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
<<<<<<< HEAD
=======
<<<<<<< HEAD
	
=======
>>>>>>> 52e9630e409f73a76cb0be7f8fc13126aca5e2ba
	public void PreTreatment(){
		String code = "function PreTreatment(col){" +
				"}";
	}
	
	
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
	public void FirstMapReduce(DBCollection col, double time){
		/*first mapreduce */		

		String mapFunc = "function(){if (this.srcgroup <= 11) " +
				"emit({srcgroup:this.srcgroup, dstgroup:this.dstgroup, app:this.app}, " +
<<<<<<< HEAD
				"{traffic:{inbyte:this.avgin*600, outbyte:this.avgout*600, conn:1}});" +
				"else " +
				"emit({srcgroup:this.dstgroup, dstgroup:this.srcgroup, app:this.app}, " +
				"{traffic:{inbyte:this.avgout*600, outbyte:this.avgin*600, conn:1}}); }";
=======
				"{traffic:{inbyte:this.traffic[0].inByte, outbyte:this.traffic[0].outByte, conn:1}});" +
				"else " +
				"emit({srcgroup:this.dstgroup, dstgroup:this.srcgroup, app:this.app}, " +
				"{traffic:{inbyte:this.traffic[0].outByte, outbyte:this.traffic[0].inByte, conn:1}}); }";
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
		String reduceFunc = "function(key, vals){var n = {inbyte:0, outbyte:0, conn:0};" +
				"for (var i in vals){" +
				"n.inbyte += vals[i].traffic.inbyte;" +
				"n.outbyte += vals[i].traffic.outbyte;" +
				"n.conn += vals[i].traffic.conn;}" +
				"return {\"traffic\":n}; }";
		DBObject query = new BasicDBObject();
<<<<<<< HEAD
		query.put("endtime", new BasicDBObject("$gt", time));
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-1", query);
//		long startTime=System.currentTimeMillis();
//		MapReduceOutput out= col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-1", query);
=======
		query.put("starttime", new BasicDBObject("$gte", time).append("$lt", time+600));
//		long startTime=System.currentTimeMillis();
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-1", query);
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
//		long endTime=System.currentTimeMillis(); 
//		int count = 0;
//	    for ( DBObject obj : out.results() ) {
//	         System.out.println( obj );
//	         count++;
//	     }
//	    System.out.println( (endTime-startTime)/1000+"s" );
//	    System.out.println( count );
	}
	
	
<<<<<<< HEAD
	public void AddKeyValues(DBCollection col){
=======
	public void AddKeyValues(DBCollection col, double time){
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
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
<<<<<<< HEAD
=======

		DBCursor ite = col.find();
		while (ite.hasNext()){
			ite.next().put("time", time);
		}
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
	}
	
	
	public void SecondMapReduce(DBCollection col){
		/*second mapreduce*/		
		
		String mapFunc = "function(){emit({srcgroup:this.srcgroup, dstgroup:this.dstgroup, app:this.app}, " +
				"{traffic:{app:this.app, inbyte:this.value.traffic.inbyte, outbyte:this.value.traffic.outbyte, " +
<<<<<<< HEAD
				"totalbyte:this.value.traffic.inbyte+this.value.traffic.outbyte, percentage:0, conn:this.value.traffic.conn, connpercentage:0}})}";
		String reduceFunc = "function(key, vals){var n = {app:\"-\", inbyte:0, outbyte:0, totalbyte:0, percentage:0, conn:0, connpercentage:0};" +
=======
				"totalbyte:this.value.traffic.inbyte+this.value.traffic.outbyte, percentage:0, conn:this.value.traffic.conn}})}";
		String reduceFunc = "function(key, vals){var n = {app:\"-\", inbyte:0, outbyte:0, totalbyte:0, percentage:0, conn:0};" +
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
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
<<<<<<< HEAD
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-2", query);
		
	}
	
	
	public void TotalCaculation(DBCollection col){
		/*caculate the totalbyte of all kinds */
		
		String mapFunc = "function(){emit({srcgroup:this._id.srcgroup, dstgroup:this._id.dstgroup}, " +
				"{totalbyte:this.value.traffic.totalbyte, conn:this.value.traffic.conn})}";
		String reduceFunc = "function(key, vals){var n = {alltotalbyte:0, totalconn:0};" +
				"for (var i in vals){" +
				"n.alltotalbyte += vals[i].totalbyte;" +
				"n.totalconn += vals[i].conn;}" +
				"return n;}";
		DBObject query = new BasicDBObject();
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-3", query);
	}
	
	
	public void AddTotal(DBCollection col1, DBCollection col2){
		/*add the key-value of tototalbyte to each document in TrafficMatrix-1-1*/	

		DBObject temp = col2.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "abroad")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "cernet")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "unicom")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "telecom")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "abroad")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "cernet")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "unicom")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "telecom")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "abroad")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "cernet")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "unicom")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "telecom")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "abroad")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "cernet")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "unicom")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = col2.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "telecom")).next();
	    col1.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
=======
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
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
	}
	
	
	public void ThirdMapReduce(DBCollection col){
		/*third mapreduce to caculate the percentage and form the last collection*/
		
		//String UTCTime = String.valueOf(time);
		String mapFunc = "function(){" +
<<<<<<< HEAD
				"this.value.traffic.percentage = this.value.traffic.totalbyte/this.total.value.alltotalbyte;" +
				"this.value.traffic.connpercentage = this.value.traffic.conn/this.total.value.totalconn;" +
				"emit({srcgroup:this._id.srcgroup, dstgroup:this._id.dstgroup}, {traffic:[this.value.traffic]}) }";
		String reduceFunc = "function(key, vals){var n = {traffic:[]};" +
				"var nn = {app:\"all\", inbyte:0, outbyte:0, totalbyte:0, percentage:1, conn:0, connpercentage:1};" +
				"for (var i in vals){" +
				"vals[i].traffic.forEach(function(traffic){" +
				"nn.inbyte += traffic.inbyte;" +
				"nn.outbyte += traffic.outbyte;" +
				"nn.totalbyte += traffic.totalbyte;" +
				"nn.conn += traffic.conn;" +
				"n.traffic.push(traffic);" +
				"});" +
				"}" +
				"n.traffic.push(nn);" +
				"return n; }";
		DBObject query = new BasicDBObject();
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-4", query);
=======
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
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix", MapReduceCommand.OutputType.MERGE, query);
	}
	
	
<<<<<<< HEAD
	public void FormTheFinalCollection(DBCollection col){
		DBCursor ite = col.find();
		DBObject temp = new BasicDBObject();
		while (ite.hasNext()){
			temp = ite.next();
			temp.put("srcgroup", "sjtu");
			temp.put("dstgroup", "wan");
			col.save(temp);
		}
		
		String mapFunc = "function(){" +
				"emit({srcgroup:this.srcgroup, dstgroup:this._id.dstgroup}, {traffic:this.value.traffic}) }";
		String reduceFunc = "function(key, vals){var n = vals[0];" +
				"for (var i = 0; i <= 4; i++){" +
				"	for (var j = 1; j <= 3; j++){" +
				"	n.traffic[i].inbyte += vals[j].traffic[i].inbyte;" +
				"	n.traffic[i].outbyte += vals[j].traffic[i].outbyte;" +
				"	n.traffic[i].totalbyte += vals[j].traffic[i].totalbyte;" +
				"	n.traffic[i].conn += vals[j].traffic[i].conn;" +
				"	}" +
				"}" +
				"for (var i in n.traffic){" +
				"n.traffic[i].percentage = n.traffic[i].totalbyte/n.traffic[4].totalbyte;" +
				"n.traffic[i].connpercentage = n.traffic[i].conn/n.traffic[4].conn;" +
				"}" +
				"return n; }";
		DBObject query = new BasicDBObject();
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix", MapReduceCommand.OutputType.MERGE, query);
		
		mapFunc = "function(){" +
				"emit({srcgroup:this._id.srcgroup, dstgroup:this.dstgroup}, {traffic:this.value.traffic}) }";
		reduceFunc = "function(key, vals){var n = vals[0];" +
				"for (var i = 0; i <= 4; i++){" +
				"	for (var j = 1; j <= 3; j++){" +
				"	n.traffic[i].inbyte += vals[j].traffic[i].inbyte;" +
				"	n.traffic[i].outbyte += vals[j].traffic[i].outbyte;" +
				"	n.traffic[i].totalbyte += vals[j].traffic[i].totalbyte;" +
				"	n.traffic[i].conn += vals[j].traffic[i].conn;" +
				"	}" +
				"}" +
				"for (var i in n.traffic){" +
				"n.traffic[i].percentage = n.traffic[i].totalbyte/n.traffic[4].totalbyte;" +
				"n.traffic[i].connpercentage = n.traffic[i].conn/n.traffic[4].conn;" +
				"}" +
				"return n; }";
		col.mapReduce(mapFunc, reduceFunc, "TrafficMatrix", MapReduceCommand.OutputType.MERGE, query);
		
=======
	public void FormTheFinalCollection(DB db, DBCollection col){
		String code = "function func(n){" +
				"}";
		DBCollection args = col;
		db.doEval(code, args);
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
	}
	
	
	public static void main(String[] args) throws UnknownHostException, MongoException{
<<<<<<< HEAD
=======
<<<<<<< HEAD
//		BufferedReader readin = new BufferedReader(new InputStreamReader(System.in));
//		try {
//			String[] read = readin.readLine().split(" ");
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
		
=======
>>>>>>> 52e9630e409f73a76cb0be7f8fc13126aca5e2ba
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
		double curTime = System.currentTimeMillis()/1000;
		double time = 1.3135914E9+Math.floor((curTime-1.3135914E9)/600-1)*600;
//		System.out.println(time);
		
<<<<<<< HEAD
=======
<<<<<<< HEAD
//		String ipAddr = "10.50.15.201";
//		String dbName = "dbpanabit";
		
		String ipAddr = args[0];
		String dbName = args[1];
=======
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
		String ipAddr = "10.50.15.210";
		String dbName = "dbpanabit";
		
//		String ipAddr = args[0];
//		String dbName = args[1];
<<<<<<< HEAD
=======
>>>>>>> 52e9630e409f73a76cb0be7f8fc13126aca5e2ba
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
		
		Mongo m = new Mongo(ipAddr);
		for (String s : m.getDatabaseNames()){
			System.out.println("DBName: "+s);
		}
		DB db = m.getDB(dbName);
		for (String s : db.getCollectionNames()){
			System.out.println("CollectionName: "+s);
		}
			
<<<<<<< HEAD
//		DBCollection col = db.getCollection("trafficSyslog");
=======
<<<<<<< HEAD
		DBCollection col = db.getCollection("trafficSyslog");
=======
//		DBCollection col = db.getCollection("trafficSyslog");
>>>>>>> 52e9630e409f73a76cb0be7f8fc13126aca5e2ba
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
//		DBCursor ite = col.find();
//		System.out.println(ite.next());
//		System.out.println(col.find(new BasicDBObject("starttime", new BasicDBObject("$gte", time).append("$lt", time+600))).count());
		
<<<<<<< HEAD
		long startTime = System.currentTimeMillis();
		
		SyslogMapReduce smr = new SyslogMapReduce();
		
		DBCollection col = db.getCollection("trafficSyslog");
		smr.FirstMapReduce(col, time);
		
		col = db.getCollection("TrafficMatrix-1");
		smr.AddKeyValues(col);
		smr.SecondMapReduce(col);
	
		col = db.getCollection("TrafficMatrix-2");
		smr.TotalCaculation(col);
		DBCollection col2 = db.getCollection("TrafficMatrix-3");
		smr.AddTotal(col, col2);
		
		smr.ThirdMapReduce(col);
		
		col = db.getCollection("TrafficMatrix-4");
		smr.FormTheFinalCollection(col);
		
		col = db.getCollection("TrafficMatrix-1");
		col.drop();
		col = db.getCollection("TrafficMatrix-2");
		col.drop();
		col = db.getCollection("TrafficMatrix-3");
		col.drop();
		col = db.getCollection("TrafficMatrix-4");
		col.drop();
		
		long endTime = System.currentTimeMillis();
		System.out.println((endTime - startTime)/1000 + "s");
=======
<<<<<<< HEAD
		long startTime = System.currentTimeMillis();
		
=======
//		long startTime = System.currentTimeMillis();
//		
>>>>>>> 52e9630e409f73a76cb0be7f8fc13126aca5e2ba
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
		
<<<<<<< HEAD
		long endTime = System.currentTimeMillis();
		System.out.println((endTime - startTime)/1000 + "s");
=======
//		long endTime = System.currentTimeMillis();
//		System.out.println((endTime - startTime)/1000 + "s");
>>>>>>> 52e9630e409f73a76cb0be7f8fc13126aca5e2ba
	
>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
		
		
		
		/*for test*/
<<<<<<< HEAD
//		col = db.getCollection("TrafficMatrix");
//		DBCursor ite = col.find();
//		DBObject temp = ite.next();
//		System.out.println(ite.count());
//		System.out.println(temp);
//		while (ite.hasNext()){
//			System.out.println(ite.next());
//		}
		
//		String code ="function func(n){" +
//				"return [n.starttime, n.endtime, n.avgin, n.avgout];}";
//		String[] str = db.doEval(code, temp).toString().split(",");
//		long starttime = Long.parseLong(str[0].substring(14), 32);
//		System.out.println(starttime);
//		System.out.println(db.command("getLastError"));
=======
//		DBCollection col = db.getCollection("TrafficMatrix");
//		DBCursor ite = col.find();
//		DBObject temp = ite.next();
//		System.out.println(temp);
//		String cde ="function func(n){" +
//				"return n.traffic[0].time;}";
//		System.out.println(db.doEval(cde, temp));

>>>>>>> 2f82e142fcb14d31a4322e032871d9d3cc214f9d
//		while (ite.hasNext()){
//			System.out.println(ite.next());
//		}

	}
}
