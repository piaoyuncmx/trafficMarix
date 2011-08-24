/**
 * 
 */
package cn.edu.sjtu.syslog.mapreduce;

import com.mongodb.DB;
import com.mongodb.DBCollection;

/**
 * @author jianwen
 *
 */
public class SyslogMapReduce {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static DBCollection syslogMapReduce(DB dbpanabit, DBCollection dbcollection, double time) {
		// TODO: fullfill it
		// Step 1: Call Phase 1 MapReduce -- Add Traffic
		// Step 2: Call Phase 2 MapReduce -- Generate Traffic Matrix
		DBCollection collAgg = MapReduceAddTraffic.addTraffic(dbpanabit, dbcollection, time);
		DBCollection collMoreAgg = MapReduceAppendKeyValues.appendKeyValues(dbpanabit, collAgg);
		DBCollection collTotal = MapReducePercentageCaculation.totalCaculation(dbpanabit, collMoreAgg);
		DBCollection collMostAgg = MapReducePercentageCaculation.percentageCaculation(dbpanabit, collMoreAgg, collTotal, time);
		DBCollection collMatrix = MapReduceTrafficMatrix.generateTrafficMatrix(dbpanabit, collMostAgg);
		
		collAgg.drop();
		collMoreAgg.drop();
		collTotal.drop();
		collMostAgg.drop();
	
		return collMatrix;
	}
	
}
