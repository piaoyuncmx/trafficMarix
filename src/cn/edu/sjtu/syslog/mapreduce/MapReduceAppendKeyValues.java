package cn.edu.sjtu.syslog.mapreduce;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MapReduceAppendKeyValues {
	public static void main(String[] args){
		
	}
	
	public static DBCollection appendKeyValues(DB dbPanabit, DBCollection collAgg){
		
		collAgg.updateMulti(new BasicDBObject("_id.srcgroup", new BasicDBObject("$lte", 2)), 
				new BasicDBObject("$set", new BasicDBObject("srcgroup", "admin")));
		collAgg.updateMulti(new BasicDBObject("_id.srcgroup", new BasicDBObject("$lte", 6).append("$gt", 2)), 
				new BasicDBObject("$set", new BasicDBObject("srcgroup", "office")));
		collAgg.updateMulti(new BasicDBObject("_id.srcgroup", new BasicDBObject("$lte", 10).append("$gt", 6)), 
				new BasicDBObject("$set", new BasicDBObject("srcgroup", "dorm")));
		collAgg.updateMulti(new BasicDBObject("_id.srcgroup", 11), 
				new BasicDBObject("$set", new BasicDBObject("srcgroup", "wireless")));
	   
		collAgg.updateMulti(new BasicDBObject("_id.dstgroup", new BasicDBObject("$lte", 257).append("$gt", 11)), 
				new BasicDBObject("$set", new BasicDBObject("dstgroup", "cernet")));
		collAgg.updateMulti(new BasicDBObject("_id.dstgroup", 258), 
				new BasicDBObject("$set", new BasicDBObject("dstgroup", "unicom")));
		collAgg.updateMulti(new BasicDBObject("_id.dstgroup", 259), 
				new BasicDBObject("$set", new BasicDBObject("dstgroup", "telecom")));
		collAgg.updateMulti(new BasicDBObject("_id.dstgroup", new BasicDBObject("$gte", 260)), 
				new BasicDBObject("$set", new BasicDBObject("dstgroup", "abroad")));
			
		collAgg.updateMulti(new BasicDBObject("_id.app", new BasicDBObject("$ne", "http").append("$ne", "bt").append("$ne", "pplive")), 
				new BasicDBObject("$set", new BasicDBObject("app", "else")));
		collAgg.updateMulti(new BasicDBObject("_id.app", "http"), 
				new BasicDBObject("$set", new BasicDBObject("app", "http")));
		collAgg.updateMulti(new BasicDBObject("_id.app", "bt"), 
				new BasicDBObject("$set", new BasicDBObject("app", "bt")));
		collAgg.updateMulti(new BasicDBObject("_id.app", "pplive"), 
				new BasicDBObject("$set", new BasicDBObject("app", "pplive")));
		
		
		String mapFunc = "function(){emit({srcgroup:this.srcgroup, dstgroup:this.dstgroup, app:this.app}, " +
				"{traffic:{app:this.app, inbyte:this.value.traffic.inbyte, outbyte:this.value.traffic.outbyte, " +
				"totalbyte:this.value.traffic.inbyte+this.value.traffic.outbyte, percentage:0, conn:this.value.traffic.conn, connpercentage:0}})}";
		String reduceFunc = "function(key, vals){var n = {app:\"-\", inbyte:0, outbyte:0, totalbyte:0, percentage:0, conn:0, connpercentage:0};" +
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
		collAgg.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-MoreAgg", query);
		
		
		DBCollection collMoreAgg = dbPanabit.getCollection("TrafficMatrix-MoreAgg");
		return collMoreAgg;
	}
}
