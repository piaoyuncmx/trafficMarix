/**
 * 
 */
package cn.edu.sjtu.syslog.mapreduce;

import com.mongodb.DBCollection;

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
	
	public static void mapReduceSyslog (DBCollection dbcollection, int time) {
		// TODO: Fullfill the functions
		// Step 1: Call 
	}
	
	public static DBCollection addTraffic(DBCollection dbcollection, int time) {
		// TODO: Fullfill the functions
		// Step 1 : Connect to temp collection
		// Step 2 : Add Traffic
		// Step 3 : Retrun the temp collection
		DBCollection collTemp = null;
		return collTemp;
	}

}
