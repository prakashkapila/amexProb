package com.amex.processors;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.amex.db.DbReader;
import com.amex.vo.Pitching;
import com.amex.vo.Rankings;

public class RankingsProcessor implements Serializable {

	private static final long serialVersionUID = 4138564348165016969L;

	public void startProcess(DbReader reader) throws IOException {
		Dataset<Row> bats = reader.read("teams")
				.select("teamID", "yearID", "Rank", "AB")
				;
		
		Dataset<Row> yearTop = bats.groupBy("yearID","teamID")
				.agg(functions.min("Rank"));
		yearTop = yearTop
				.withColumnRenamed("yearID", "minYearID")
				.withColumnRenamed("teamID", "minTeamID")
					.withColumnRenamed("min(Rank)", "RankMin")
					.groupBy("minYearID","minTeamID","RankMin")
					.count();
	 	
		Dataset<Row> yearBot = bats.select("teamID","yearID","Rank","AB")
				.groupBy("teamID","yearID","Rank","AB").max("Rank");
		
		yearBot = yearBot.withColumnRenamed("max(Rank)", "RankMax").sort(new Column("RankMax").desc());
		yearBot.show();
		Dataset<Row> result = yearTop.join(yearBot,new Column("minYearID")
				.equalTo(new Column("yearID")));
		result.show();
		
//		Dataset<Rankings> rankings = bats.map(new MapFunction<Row, Rankings>() {
//			private static final long serialVersionUID = 595291736651302679L;
// 			@Override
//			public Rankings call(Row value) throws Exception {
//				Rankings rank = new Rankings();
//				rank.apply(value);
//				return rank;
//			}
//		}, Encoders.bean(Rankings.class));
		Dataset<Rankings> rankings = result.flatMap(new FlatMapFunction<Row,Rankings>(){
 	@Override
			public Iterator<Rankings> call(Row t) throws Exception {
 				List<Rankings> rankings = new ArrayList<Rankings>();
 				Rankings rank = new Rankings();
 				Rankings rank2 = new Rankings();
 				rank.apply(t);
 				rank2.apply(t.getString(4),t.getString(5),t.getString(6),t.getString(7));
 				rankings.add(rank);
 				rankings.add(rank2);
				 return rankings.iterator();
			}}, Encoders.bean(Rankings.class));
		
//		bats.takeAsList(2).forEach(x -> {
//			Rankings ranks = new Rankings();
//			ranks.apply(x);
//			System.out.println(ranks.toString());
//		});

	}

	private void saveFile(Dataset<Rankings> pitching, final FileWriter writer) throws IOException {
		writer.write(Rankings.COLUMNS());
		writer.flush();
		CSVWriter.writer = writer;
		pitching.foreach(new RankingsCSVWriter());
	}

	private void saveFile(Dataset<Rankings> pitching) throws IOException {
		File file = new File("Rankings" + System.currentTimeMillis() + ".csv");
		file.createNewFile();
		System.out.println("saving to file" + file);
		FileWriter writer = new FileWriter(file);
		saveFile(pitching, writer);

	}

	public static void startProcessor(DbReader dbReader) {
		RankingsProcessor processor = new RankingsProcessor();
		try {
			processor.startProcess(dbReader);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String arg[]) throws IOException {
		Logger.getLogger("org.apache").setLevel(Level.ERROR);
		RankingsProcessor proce = new RankingsProcessor();
		proce.startProcess(new DbReader());
		
	}
}

class RankingsCSVWriter implements ForeachFunction<Rankings> {

	private static final long serialVersionUID = 6319324370196471909L;
	static FileWriter writer = null;

	@Override
	public void call(Rankings t) throws Exception {
		writer.write(t != null ? t.toString() : "null");
		writer.flush();
	}
}
