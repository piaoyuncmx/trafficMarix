/**
 * 
 */
package cn.edu.sjtu.syslog.mapreduce;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;

/**
 * @author jianwen
 *
 */
public class MapReduceTrafficMatrix {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static DBCollection generateTrafficMatrix (DB dbPanabit, DBCollection collMostAgg) {
		// TODO: Fullfill the functions
		// Step 1 : Connect to aggTraffic collection
		// Step 2 : Generate Traffic Matrix
		// Step 3 : Return the Matrix collection
		
		DBCursor ite = collMostAgg.find();
		DBObject temp = new BasicDBObject();
		while (ite.hasNext()){
			temp = ite.next();
			temp.put("srcgroup", "sjtu");
			temp.put("dstgroup", "wan");
			collMostAgg.save(temp);
		}
		
		String mapFunc = "function(){" +
				"emit({srcgroup:this.srcgroup, dstgroup:this._id.dstgroup, this._id.time}, {traffic:this.value.traffic}) }";
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
		collMostAgg.mapReduce(mapFunc, reduceFunc, "TrafficMatrix", MapReduceCommand.OutputType.MERGE, query);
		
		mapFunc = "function(){" +
				"emit({srcgroup:this._id.srcgroup, dstgroup:this.dstgroup, this._id.time}, {traffic:this.value.traffic}) }";
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
		collMostAgg.mapReduce(mapFunc, reduceFunc, "TrafficMatrix", MapReduceCommand.OutputType.MERGE, query);
		
		mapFunc = "function(){" +
				"emit({srcgroup:this.srcgroup, dstgroup:this.dstgroup, this._id.time}, {traffic:this.value.traffic}) }";
		reduceFunc = "function(key, vals){var n = vals[0];" +
				"for (var i = 0; i <= 4; i++){" +
				"	for (var j = 1; j <= 15; j++){" +
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
		collMostAgg.mapReduce(mapFunc, reduceFunc, "TrafficMatrix", MapReduceCommand.OutputType.MERGE, query);
				
		DBCollection collMatrix = dbPanabit.getCollection("TrafficMatrix");
		return collMatrix;
	}
	
}
