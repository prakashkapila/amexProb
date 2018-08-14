package com.amex.processors;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.amex.db.DbReader;
import com.amex.vo.HallOfFame;
import java.io.Serializable;

public class HallOfFameERAProcessor implements Serializable {
	/*
Calculate the number of all star appearances for each Hall of Fame pitcher 
and their average ERA their all-star years and list the year they were 
inducted into the Hall of Fame
*/
	private static final long serialVersionUID = -270031695649461322L;
	
	private Dataset<Row> getHallOfFameSet(DbReader reader){
		return reader.read("halloffame");
	}
	
	private Dataset<Row> getAllStarAppearances(DbReader reader){
		return reader.read("allstarfull");
	}
	private Dataset<Row> getPitching(DbReader reader){
		return reader.read("pitching");
	}
	
	public void startProcess(DbReader reader) throws IOException {
		
		StringBuilder sqlbuilder =new StringBuilder()
				.append("select a.playerID,a.ERA,a.yearID from Pitching a, AllstarFull b, HallOfFame c  ")
				//.append(" allstarfull b, halloffame c")
			    .append(" where a.playerID = b.playerid")
			    .append(" and a.playerid=c.playerid")
			    .append(" and c.inducted='Y'")
			    .append( "and c.YearID=b.YearID");
		
		Dataset<Row> halloffame = getHallOfFameSet(reader)
				.filter(new Column("inducted").equalTo("Y"))
				.withColumnRenamed("YearID", "InductedYear")
				;
		
 		
		Dataset<Row> allStarAppearances = getAllStarAppearances(reader);
		Dataset<Row> pitching = getPitching(reader);
		
		pitching.createOrReplaceTempView("pitching");
		allStarAppearances.createOrReplaceTempView("allstarfull");
		halloffame.createOrReplaceTempView("halloffame");
		
		
		Dataset<Row> countFameAppearances = halloffame.join(allStarAppearances,"playerID");

		countFameAppearances=countFameAppearances.groupBy("playerID").count();
		countFameAppearances=countFameAppearances.withColumnRenamed("count", "appearances");
		
		countFameAppearances = countFameAppearances.join(halloffame,"playerID");
		
 		countFameAppearances = countFameAppearances.join(allStarAppearances,"playerID")
		.groupBy("playerID","YearID","InductedYear","appearances").count();

		countFameAppearances.show();
		
		countFameAppearances = countFameAppearances.withColumnRenamed("playerID", "FameplayerID")
				.withColumnRenamed("YearID", "appearYearID");
		Dataset<Row> result1=  countFameAppearances
				.join(
						pitching,pitching.col("playerID").equalTo(countFameAppearances.col("FameplayerID"))
						.and (pitching.col("YearID").equalTo(countFameAppearances.col("appearYearID")))
						)
				.groupBy("playerID","YearID","InductedYear","appearances")
				.avg("ERA");
		result1.show();
//		
////		Dataset<Row> result = DbReader.getSession().sql(sqlbuilder.toString());
////		result.show();
		saveFile(result1.map(new MapFunction<Row,HallOfFame>(){
  			private static final long serialVersionUID = 1L;
 			@Override
			public HallOfFame call(Row value) throws Exception {
			 	HallOfFame hall = new HallOfFame();
				hall.apply(value);
				return hall;
			}
 		}, Encoders.bean(HallOfFame.class)));
		
	}
	private void saveFile(Dataset<HallOfFame> avgSals,final FileWriter writer) throws IOException
	{
 		writer.write(HallOfFame.COLUMNS());
		writer.flush();
		HallOfFameWriter.writer = writer;
		avgSals.foreach(new HallOfFameWriter());
	}
	private void saveFile(Dataset<HallOfFame> avgSals) throws IOException
	{
		File file = new File("HallOfFame"+System.currentTimeMillis()+".csv");
		//file.mkdirs();
		file.createNewFile();
		System.out.println("saving to file"+file);
		FileWriter writer = new FileWriter(file);
		saveFile(avgSals,writer); 
				
	}
	public static void startProcessor(DbReader reader) {
	 	HallOfFameERAProcessor proce = new HallOfFameERAProcessor();
		try {
			proce.startProcess(reader);
		} catch (IOException e) {
	 		e.printStackTrace();
		}
	}
	
	public static void main(String[] arg)
	{
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		startProcessor(new DbReader());
	}
}
class HallOfFameWriter implements ForeachFunction<HallOfFame> {

	private static final long serialVersionUID = 6319324370196471908L;
	static FileWriter writer = null;
 	@Override
	public void call(HallOfFame t) throws Exception {
		writer.write(t != null ? t.toString() : "null");
		writer.flush();
	}
}