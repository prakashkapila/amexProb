package com.amex.processors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.amex.db.DbReader;

public class PitchingProcessor {
	/*
	 * Calculate the top 10 pitchers' average regular season and post-season ERAs
	 * and average win/loss (w/(w+l)) percentages i.e. The top 10 pitcher's ERAs
	 * ((0.65(player1) + 0.72(player2) + ...) / 10) and (win/loss of player1 +
	 * win/loss of player2 + ...)/10 The pitchers in the top 10 may not have been on
	 * a team that made it to the post-season, so average the ERAs & win/loss of the
	 * pitchers that made it into the post-season
	 */

	public void startProcess(DbReader reader) {
		Dataset<Row> regular =getRegularSeasonPitching(reader,10);
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
		Dataset<Row> regular =reader.read("pitchingpost")
				.groupBy("playerID")
				.avg("ERA")
				.withColumnRenamed("avg(ERA)", "avgERA")
				.sort(new Column("avgERA").desc())
				.limit(num);
		return regular.select("playerID","W","L","ERA");
	}

	private Dataset<Row> getRegularSeasonPitching(DbReader reader,int num) {
		Dataset<Row> regular = reader.read("pitching").
				groupBy("playerID")
				.avg("ERA")
				.withColumnRenamed("avg(ERA)", "avgERA")
				.sort(new Column("avgERA").desc())
				.limit(num);
		return regular.select("playerID","W","L","ERA");
 	}
}
