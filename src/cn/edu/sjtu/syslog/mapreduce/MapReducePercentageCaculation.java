package cn.edu.sjtu.syslog.mapreduce;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;

public class MapReducePercentageCaculation {
	public static void main(String[] args){
		
	}
	
	public static DBCollection totalCaculation(DB dbpanabit, DBCollection dbcollection){
		String mapFunc = "function(){emit({srcgroup:this._id.srcgroup, dstgroup:this._id.dstgroup}, " +
				"{totalbyte:this.value.traffic.totalbyte, conn:this.value.traffic.conn})}";
		String reduceFunc = "function(key, vals){var n = {alltotalbyte:0, totalconn:0};" +
				"for (var i in vals){" +
				"n.alltotalbyte += vals[i].totalbyte;" +
				"n.totalconn += vals[i].conn;}" +
				"return n;}";
		DBObject query = new BasicDBObject();
		dbcollection.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-Total", query);
		
		DBCollection collTotal = dbpanabit.getCollection("TrafficMatrix-Total");
		return collTotal;
	}
	
	public static DBCollection percentageCaculation(DB dbpanabit, DBCollection collMoreAgg, DBCollection collTotal, double time){
		DBObject temp = collTotal.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "abroad")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "cernet")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "unicom")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "telecom")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "admin").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "abroad")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "cernet")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "unicom")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "telecom")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "office").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "abroad")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "cernet")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "unicom")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "telecom")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "dorm").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "abroad")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "abroad"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "cernet")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "cernet"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "unicom")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "unicom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    temp = collTotal.find(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "telecom")).next();
	    collMoreAgg.updateMulti(new BasicDBObject("_id.srcgroup", "wireless").append("_id.dstgroup", "telecom"), 
	    				new BasicDBObject("$set", new BasicDBObject("total", temp)));
	    
		DBCursor ite = collMoreAgg.find();
		DBObject tmp = new BasicDBObject();
		while (ite.hasNext()){
			tmp = ite.next();
			tmp.put("time", time);
			collMoreAgg.save(tmp);
		}
	    
		String mapFunc = "function(){" +
				"this.value.traffic.percentage = this.value.traffic.totalbyte/this.total.value.alltotalbyte;" +
				"this.value.traffic.connpercentage = this.value.traffic.conn/this.total.value.totalconn;" +
				"emit({srcgroup:this._id.srcgroup, dstgroup:this._id.dstgroup, time:this.time}, {traffic:[this.value.traffic]}) }";
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
		collMoreAgg.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-MostAgg", query);
		collMoreAgg.mapReduce(mapFunc, reduceFunc, "TrafficMatrix", MapReduceCommand.OutputType.MERGE, query);
		
		DBCollection collMostAgg = dbpanabit.getCollection("TrafficMatrix-MostAgg");
		return collMostAgg;
	}
}
