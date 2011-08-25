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

	public static DBCollection syslogMapReduce(DB dbPanabit, DBCollection dbCollection, double time) {
		// TODO: fullfill it
		// Step 1: Call Phase 1 MapReduce -- Add Traffic
		// Step 2: Call Phase 2 MapReduce -- Generate Traffic Matrix
		DBCollection collAgg = MapReduceAddTraffic.addTraffic(dbPanabit, dbCollection, time);
		DBCollection collMoreAgg = MapReduceAppendKeyValues.appendKeyValues(dbPanabit, collAgg);
		DBCollection collTotal = MapReducePercentageCaculation.totalCaculation(dbPanabit, collMoreAgg);
		DBCollection collMostAgg = MapReducePercentageCaculation.percentageCaculation(dbPanabit, collMoreAgg, collTotal, time);
		DBCollection collMatrix = MapReduceTrafficMatrix.generateTrafficMatrix(dbPanabit, collMostAgg);
		
		collAgg.drop();
		collMoreAgg.drop();
		collTotal.drop();
		collMostAgg.drop();
		
		MapReduceRemoveOld.removeOld(dbCollection, time);
	
		return collMatrix;
	}
	
}
