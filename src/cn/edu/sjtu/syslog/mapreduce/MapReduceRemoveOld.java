package cn.edu.sjtu.syslog.mapreduce;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryOperators;

public class MapReduceRemoveOld {
	public static void main(String[] args){
		
	}
	
	public static void removeOld(DBCollection dbCollection, double time){
		DBObject query = new BasicDBObject("starttime", new BasicDBObject("$lte", time+600)).
									append("endtime", new BasicDBObject("$gt", time)).
									append("app", new BasicDBObject(QueryOperators.IN, new String[] {"bt", "synack"}));
		
		DBCursor ite = dbCollection.find(query);
		while (ite.hasNext()){
			dbCollection.remove(ite.next());
		}
	}
}
