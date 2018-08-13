package com.amex.processors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.amex.db.DbReader;

public class PitchingProcessor {
	/*
	 * Calculate the top 10 pitchers' average regular season and post-season ERAs
	 * and average win/loss (w/(w+l)) percentages i.e. The top 10 pitcher's ERAs
	 * ((0.65(player1) + 0.72(player2) + ...) / 10) and (win/loss of player1 +
	 * win/loss of player2 + ...)/10 
	 * The pitchers in the top 10 may not have been on
	 * a team that made it to the post-season, so average the ERAs & win/loss of the
	 * pitchers that made it into the post-season
	 */

	public void startProcess(DbReader reader) {
		Dataset<Row> regular =getRegularSeasonPitching(reader,10);
		regular.groupBy("playerID").sum("W","L").withColumnRenamed("sum(W)", "Wins")
		.withColumnRenamed("sum(L)", "Loss").withColumn("total", functions.expr("Wins+Loss")).show();
	
		regular = regular.withColumnRenamed("W", "regularWins")
		.withColumnRenamed("L", "regularLosses")
		.withColumnRenamed("ERA", "regularERA");
		
		regular.show();
	 	
	 	Dataset<Row> post =getPostSeasonPitching(reader,10);
	 	post = post.withColumnRenamed("W", "postWins")
	 			.withColumnRenamed("L", "postLosses")
	 			.withColumnRenamed("ERA", "postERA");
	 	post.show();
	 	
	 	regular = regular.join(post,"playerID");
	 	regular.show();
 	}

	private Dataset<Row> getPostSeasonPitching(DbReader reader,int num) {
 				Dataset<Row> regular = reader.read("pitchingpost")//.
			 			.groupBy("playerID","W","L","ERA")
						.avg("ERA")
						.withColumnRenamed("avg(ERA)", "avgERA")
						.sort(new Column("avgERA").desc())
						.limit(num);
		 		return regular;
	}

	private Dataset<Row> getData(DbReader reader,int num,String table)
	{
		Dataset<Row> regular = reader.read(table);
		Dataset<Row> eraGroup = regular.filter(functions.expr("ERA > 0"))
	 			.groupBy("ERA","playerID")
	 			.avg("ERA")
	 			//.withColumn("samp", functions.avg("ERA"))//.
				.withColumnRenamed("avg(ERA)", "avgERA")
				.sort(new Column("avgERA"))
				.limit(num);
		regular = regular.join(eraGroup,"playerID")
				.groupBy("playerID","avgERA").agg(functions.sum("W"),functions.sum("L"))
				.withColumnRenamed("sum(W)", "totalWins")
				.withColumnRenamed("sum(L)", "totalLoss")
				.withColumn("total", functions.expr("totalWins+totalLoss"))
				.withColumn("WinPercent", functions.expr("totalWins/total"))
				.withColumn("LossPercent", functions.expr("totalLoss/total"));
		System.out.println("The total count is"+regular.count());
		regular.show();
 		return regular;
	}
	private Dataset<Row> getRegularSeasonPitching(DbReader reader,int num) {
		
		return 	getData(reader, num, "pitching");
 	}
	
	public static void main(String arg[])
	{
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		PitchingProcessor proce = new PitchingProcessor();
		proce.startProcess(new DbReader());
	}

	public static void startProcessor(DbReader dbReader) {
		PitchingProcessor proce = new PitchingProcessor();
		proce.startProcess(new DbReader());
	}
}
