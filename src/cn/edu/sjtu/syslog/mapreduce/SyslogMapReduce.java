/**
 * 
 */
package cn.edu.sjtu.syslog.mapreduce;

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

	public static DBCollection syslogMapReduce(DBCollection dbcollection, int time) {
		// TODO: fullfill it
		// Step 1: Call Phase 1 MapReduce -- Add Traffic
		// Step 2: Call Phase 2 MapReduce -- Generate Traffic Matrix
		DBCollection collAgg = MapReduceAddTraffic.addTraffic(dbcollection, time);
		DBCollection collMatrix = MapReduceTrafficMatrix.generateTrafficMatrix(dbcollection, time);
		return collMatrix;
	}
	
}
