/**
 * 
 */
package cn.edu.sjtu.syslog.mapreduce;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * @author jianwen
 * Phase 1 MapReduce: Add traffic with same srcgroup, dstgroup, app
 */
public class MapReduceAddTraffic {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public static void mapReduceSyslog (DBCollection dbcollection, double time) {
		// TODO: Fullfill the functions
		// Step 1: Call 
	}
	
	public static DBCollection addTraffic(DB dbpanabit, DBCollection dbcollection, double time) {
		// TODO: Fullfill the functions
		// Step 1 : Connect to temp collection
		// Step 2 : Add Traffic
		// Step 3 : Retrun the temp collection
		
		/*First MapReduce*/
		String mapFunc = "function(){if (this.srcgroup <= 11) " +
				"emit({srcgroup:this.srcgroup, dstgroup:this.dstgroup, app:this.app}, " +
				"{traffic:{inbyte:this.avgin*600, outbyte:this.avgout*600, conn:1}});" +
				"else " +
				"emit({srcgroup:this.dstgroup, dstgroup:this.srcgroup, app:this.app}, " +
				"{traffic:{inbyte:this.avgout*600, outbyte:this.avgin*600, conn:1}}); }";
		String reduceFunc = "function(key, vals){var n = {inbyte:0, outbyte:0, conn:0};" +
				"for (var i in vals){" +
				"n.inbyte += vals[i].traffic.inbyte;" +
				"n.outbyte += vals[i].traffic.outbyte;" +
				"n.conn += vals[i].traffic.conn;}" +
				"return {\"traffic\":n}; }";
		DBObject query = new BasicDBObject();
		query.put("starttime", new BasicDBObject("$lte", time+600));
		query.put("endtime", new BasicDBObject("$gt", time));
		dbcollection.mapReduce(mapFunc, reduceFunc, "TrafficMatrix-Agg", query);

		DBCollection collAgg = dbpanabit.getCollection("TrafficMatrix-Agg");
		return collAgg;
	}

}
