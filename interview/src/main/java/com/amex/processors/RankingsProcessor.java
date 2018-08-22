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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

import com.amex.db.DbReader;
import com.amex.vo.Pitching;
import com.amex.vo.Rankings;

import scala.Tuple2;

public class RankingsProcessor implements Serializable {

	private static final long serialVersionUID = 4138564348165016969L;

	public void startProcess(DbReader reader) throws IOException {
		Dataset<Row> bats = reader.read("teams")
				.select("teamID", "yearID", "Rank", "AB")
				;
		
		Dataset<Row> yearTop = bats.groupBy("yearID","teamID","AB")
				.agg(functions.min("Rank"));
 		yearTop = yearTop
				.withColumnRenamed("yearID", "minYearID")
				.withColumnRenamed("teamID", "minTeamID")
				.withColumnRenamed("AB", "minAtBats")
					.withColumnRenamed("min(Rank)", "RankMin");
		 
		Dataset<Row> yearBot = bats.select("teamID","yearID","Rank","AB")
				.groupBy("teamID","yearID","AB").max("Rank");
		
		Dataset<Row> yearBotSorted = yearBot.withColumnRenamed("max(Rank)", "RankMax")
									.sort(new Column("RankMax").desc());
		yearTop.show();
	 
		Dataset<Row> result = yearTop
				.join(yearBotSorted,new Column("minYearID")
				.equalTo(new Column("yearID")));
		
  		Dataset<Rankings> ranks = result.flatMap(new FlatMapFunction<Row, Rankings>(){
  			private static final long serialVersionUID = 595291736651302679L;
 			@Override
			public Iterator<Rankings> call(Row value) throws Exception {
 				List<Rankings> iter = new ArrayList<Rankings>();
 				Rankings rank = new Rankings();
 				rank.apply(value);
 				Rankings rank1 = new Rankings();
 				rank1.apply(value.get(4),value.get(5),value.get(6),value.get(7));
 				iter.add(rank);
 				iter.add(rank1);
 				return iter.iterator();
			}},Encoders.bean(Rankings.class));		
  		ranks.show();
  		System.out.println("ranks count is "+ranks.count());
  		
  		JavaPairRDD<Integer,Rankings> rankInts = ranks.javaRDD().
  				mapToPair(new PairFunction<Rankings,Integer,Rankings>(){
  					private static final long serialVersionUID = 23448160171039937L;
 					@Override
					public Tuple2<Integer, Rankings> call(Rankings t) throws Exception {
 						return new Tuple2<Integer,Rankings>(t.getYear(),t);
					}});
  		
  	 	JavaPairRDD<Integer,List<Rankings>> minMaxRanks = 
  				rankInts.groupByKey().mapValues(new Function<Iterable<Rankings>,List<Rankings>>(){
 					private static final long serialVersionUID = -8906497887177445436L;
 			@Override
			public List<Rankings> call(Iterable<Rankings> v1) throws Exception {
 				List<Rankings> rakes = new ArrayList<Rankings>();
				int min=100,max=0;
				List<Rankings> lowest = new ArrayList<Rankings>();
				List<Rankings> highest = new ArrayList<Rankings>();
		 		for(Rankings x:v1)
				{
					if(min > x.getMinRank()) {
						min =x.getMinRank();
						lowest.clear();
						lowest.add(x);
					 }
					else {
						if(min ==x.getMinRank() &&  (!lowest.contains(x)))
						{
							lowest.add(x);
						}
					}
					if(max < x.getMinRank()) {
						max =x.getMinRank();
						max = x.getMinRank();
						highest.clear();
							highest.add(x);
					 }else
						 if(max == x.getMinRank() && (!highest.contains(x)))
						 {
							 highest.add(x);
						 }
					
				}
				rakes.addAll(highest);
				rakes.addAll(lowest);
 				return rakes;
			}
  		}
  		); 		
  		System.out.println("minMaxRanks count is "+minMaxRanks.count());
  		minMaxRanks.take(20).forEach(x->{
  			x._2.forEach(y->System.out.println(y.toString()));
  		});
  		JavaRDD<Rankings> resultRankings = 
  				minMaxRanks.flatMap(new FlatMapFunction<Tuple2<Integer,List<Rankings>>,Rankings>(){

			/**
					 * 
					 */
					private static final long serialVersionUID = 2734868681816226078L;

			@Override
			public Iterator<Rankings> call(Tuple2<Integer, List<Rankings>> t) throws Exception {
	 			return t._2.iterator();
			}});
  		SQLContext sqlc = new SQLContext(result.sparkSession());
  		Dataset<Rankings> ranksFinal = sqlc.createDataFrame(resultRankings, Rankings.class).map(new MapFunction<Row,Rankings>(){
 	 		private static final long serialVersionUID = 3332200811591838003L;
 			@Override
			public Rankings call(Row value) throws Exception {
				Rankings ranks = new Rankings();
				ranks.apply(value);
				return ranks;
			}}, Encoders.bean(Rankings.class));
  		saveFile(ranksFinal.orderBy("year"));
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
		Logger.getLogger("org.apache").setLevel(Level.OFF);
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
