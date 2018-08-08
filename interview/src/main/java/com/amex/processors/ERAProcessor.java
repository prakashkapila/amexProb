package com.amex.processors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.amex.db.DbReader;

public class ERAProcessor {
	
	private Dataset<Row> getHallOfFameSet(DbReader reader){
		return reader.read("halloffame");
	}
	
	private Dataset<Row> getAllStarAppearances(DbReader reader){
		return reader.read("allstarfull");
	}
	private Dataset<Row> getPitching(DbReader reader){
		return reader.read("pitching");
	}
	
	public void startProcess() {
		DbReader reader = new DbReader();
		StringBuilder sqlbuilder =new StringBuilder()
				.append("select a.playerID,a.ERA,a.yearID from pitching a,  ")
				.append("allstarfull b, halloffame c")
			    .append("where a.playerid = b.playerid")
			    .append("and a.playerid=c.playerid")
			    .append("and c.inducted='Y'");
		
		Dataset<Row> halloffame = getHallOfFameSet(reader);
		Dataset<Row> allStarAppearances = getAllStarAppearances(reader);
		
		Dataset<Row> pitching = getPitching(reader).filter(new Column("inducted").equalTo("Y"));
		
		pitching.createOrReplaceTempView("pitching");
		allStarAppearances.createOrReplaceTempView("allstarfull");
		halloffame.createOrReplaceTempView("halloffame");
		
		
		Dataset<Row> countFameAppearances = halloffame.join(allStarAppearances,"playerID")
											.groupBy("playerID").count();
		countFameAppearances.show();
		
		Dataset<Row> result1=  countFameAppearances.join(pitching,"playerID");
		result1.show();
		
		Dataset<Row> result = DbReader.getSession().sql(sqlbuilder.toString());
		result.show();
	}
}
