package com.amex.processors;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;

import com.amex.db.DbReader;
import com.amex.vo.Pitching;

public class PitchingProcessor implements Serializable {
	/*
	 * Calculate the top 10 pitchers' average regular season and post-season ERAs
	 * and average win/loss (w/(w+l)) percentages i.e. The top 10 pitcher's ERAs
	 * ((0.65(player1) + 0.72(player2) + ...) / 10) and (win/loss of player1 +
	 * win/loss of player2 + ...)/10 
	 * The pitchers in the top 10 may not have been on
	 * a team that made it to the post-season, so average the ERAs & win/loss of the
	 * pitchers that made it into the post-season
	 */

	/**
	 * 
	 */
	private static final long serialVersionUID = -1129645751543755808L;

	public void startProcess(DbReader reader) throws IOException {
		Dataset<Row> regular =getRegularSeasonPitching(reader,10); 
  	 	Dataset<Row> post =getPostSeasonPitching(reader, 10);
 	 	regular = regular.join(post,"playerID").groupBy("playerID","YearID","RegularavgERA","PostavgERA","RegularWinPercent","PostWinPercent","RegularLossPercent","PostLossPercent")
 	 			//.agg(functions.avg("PostavgERA"), functions.sum("RegularWins"),functions.sum("PostWins"),functions.sum("RegularLosses"));
 	 			.count();
 	 			regular.show();
	 	Dataset<Pitching> pitching = 
	 			regular.map(new MapFunction<Row,Pitching>(){
	 				private static final long serialVersionUID = 498803601214462167L;
 					@Override
					public Pitching call(Row value) throws Exception {
 						Pitching pitch = new Pitching();
 						pitch.apply(value);
 						return pitch;
					}}, Encoders.bean(Pitching.class));
	 	pitching.show();
	 	pitching.persist(StorageLevel.DISK_ONLY());
	 	saveFile(pitching);
 	}
	private void saveFile(Dataset<Pitching> pitching,final FileWriter writer) throws IOException
	{
 		writer.write(Pitching.COLUMNS());
		writer.flush();
		CSVWriter.writer = writer;
		pitching.foreach(new CSVWriter());
	}
	
	private void saveFile(Dataset<Pitching> pitching) throws IOException
	{
		File file = new File("Pitching"+System.currentTimeMillis()+".csv");
 		file.createNewFile();
		System.out.println("saving to file"+file);
		FileWriter writer = new FileWriter(file);
		saveFile(pitching,writer); 
				
	}
private Dataset<Row> getRegularSeasonPitching(DbReader reader,int num) {
	Dataset<Row> ret = getData(reader,num,"pitchingpost");//.
	ret = ret.withColumnRenamed("totalWins", "RegularWins")
			.withColumnRenamed("avgERA", "RegularavgERA")	
			.withColumnRenamed("YearID", "RegularYearID")	
			.withColumnRenamed("totalLoss", "RegularLosses")
			.withColumnRenamed("WinPercent", "RegularWinPercent")
			.withColumnRenamed("LossPercent", "RegularLossPercent");
	
		return 	ret;
 	}
	private Dataset<Row> getPostSeasonPitching(DbReader reader,int num) {
			Dataset<Row> ret = getData(reader,num,"pitchingpost");//.
			ret = ret.withColumnRenamed("totalWins", "PostWins")
					.withColumnRenamed("avgERA", "PostavgERA")					
					.withColumnRenamed("totalLoss", "PostLosses")
					.withColumnRenamed("WinPercent", "PostWinPercent")
					.withColumnRenamed("LossPercent", "PostLossPercent");
 				return ret;
	}

	private Dataset<Row> getData(DbReader reader,int num,String table)
	{
		Dataset<Row> regular = reader.read(table);
		Dataset<Row> eraGroup = regular
				.withColumn("ERAC", regular.col("ERA").cast(DataTypes.DoubleType))
				.filter(functions.expr("ERAC > 0"))
	 			.groupBy("ERAC","playerID")
	 			.avg("ERAC")
	 			//.withColumn("samp", functions.avg("ERA"))//.
				.withColumnRenamed("avg(ERAC)", "avgERA")
				.sort(new Column("avgERA"))
				.limit(num);
		
		regular = regular.join(eraGroup,"playerID")
				.groupBy("YearID","playerID","avgERA").agg(functions.sum("W"),functions.sum("L"))
				.withColumnRenamed("sum(W)", "totalWins")
				.withColumnRenamed("sum(L)", "totalLoss")
				.withColumn("total", functions.expr("totalWins+totalLoss"))
				.withColumn("WinPercent", functions.expr("totalWins/total"))
				.withColumn("LossPercent", functions.expr("totalLoss/total"));
		regular.show();
 		return regular.na().drop();
	}
	
	
	public static void main(String arg[])
	{
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		PitchingProcessor proce = new PitchingProcessor();
		try {
			proce.startProcess(new DbReader());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void startProcessor(DbReader dbReader) {
		PitchingProcessor proce = new PitchingProcessor();
		try {
			proce.startProcess(new DbReader());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class CSVWriter implements ForeachFunction<Pitching>
{
	
	private static final long serialVersionUID = 6319324370196471909L;
	static  FileWriter writer = null;
	@Override
	public void call(Pitching t) throws Exception {
		 writer.write(t != null ? t.toString():"null");
		 writer.flush();
	}
	}