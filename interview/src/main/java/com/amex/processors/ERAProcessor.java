package com.amex.processors;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import com.amex.db.DbReader;
import com.amex.vo.AverageSalaries;
import com.amex.vo.Pitching;

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
	
	public void startProcess(DbReader reader) throws IOException {
		
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
		saveFile(result.map(new MapFunction<Row,Pitching>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Pitching call(Row value) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
			
		}, Encoders.bean(Pitching.class)));
	}
	private void saveFile(Dataset<Pitching> avgSals,final FileWriter writer) throws IOException
	{
 		writer.write(AverageSalaries.COLUMNS());
		writer.flush();
		avgSals.foreach(x->writer.write(x.toString()));
	}
	private void saveFile(Dataset<Pitching> avgSals) throws IOException
	{
		File file = new File("/AvgSalaries"+System.currentTimeMillis()+".csv");
		file.mkdirs();
		file.createNewFile();
		System.out.println("saving to file"+file);
		FileWriter writer = new FileWriter(file);
		saveFile(avgSals,writer); 
				
	}
	public static void startProcessor(DbReader reader) {
		// TODO Auto-generated method stub
		ERAProcessor proce = new ERAProcessor();
		try {
			proce.startProcess(reader);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String arg)
	{
		startProcessor(new DbReader());
	}
}
