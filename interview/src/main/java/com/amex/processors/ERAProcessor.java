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
import com.amex.vo.AverageSalaries;
import com.amex.vo.HallOfFame;
import com.amex.vo.Pitching;

import scala.Function0;
import scala.Function1;
import scala.Function2;
import scala.Option;
import scala.PartialFunction;
import scala.Predef.$less$colon$less;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.GenIterable;
import scala.collection.GenSeq;
import scala.collection.GenTraversable;
import scala.collection.GenTraversableOnce;
import scala.collection.Iterable;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.SeqView;
import scala.collection.Traversable;
import scala.collection.TraversableOnce;
import scala.collection.generic.CanBuildFrom;
import scala.collection.generic.FilterMonadic;
import scala.collection.generic.GenericCompanion;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.List;
import scala.collection.immutable.Map;
import scala.collection.immutable.Range;
import scala.collection.immutable.Set;
import scala.collection.immutable.Stream;
import scala.collection.immutable.Vector;
import scala.collection.mutable.Buffer;
import scala.collection.mutable.Builder;
import scala.collection.parallel.Combiner;
import scala.collection.parallel.ParIterable;
import scala.math.Numeric;
import scala.math.Ordering;
import scala.reflect.ClassTag;
import scala.runtime.Nothing$;

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
				.append("select a.playerID,a.ERA,a.yearID from Pitching a, AllstarFull b, HallOfFame c  ")
				//.append(" allstarfull b, halloffame c")
			    .append(" where a.playerID = b.playerid")
			    .append(" and a.playerid=c.playerid")
			    .append(" and c.inducted='Y'")
			    .append( "and c.YearID=b.YearID");
		
		Dataset<Row> halloffame = getHallOfFameSet(reader)
				.filter(new Column("inducted").equalTo("Y"))
				//.withColumnRenamed("YearID", "InductedYear")
				;
		
 		
		Dataset<Row> allStarAppearances = getAllStarAppearances(reader);
		allStarAppearances = allStarAppearances.join(
				halloffame,
				(allStarAppearances.col("playerID").equalTo(halloffame.col("playerID")))
				.and(allStarAppearances.col("YearID").equalTo(halloffame.col("YearID")))
				)
				;
		System.out.println("Count is "+allStarAppearances.count());
		allStarAppearances.show();
		Dataset<Row> pitching = getPitching(reader);
		
		pitching.createOrReplaceTempView("pitching");
		allStarAppearances.createOrReplaceTempView("allstarfull");
		halloffame.createOrReplaceTempView("halloffame");
		
		
		Dataset<Row> countFameAppearances = halloffame.join(allStarAppearances,"playerID")
											.groupBy("playerID","YearID","ERA").count();
		countFameAppearances.show();
		
		Dataset<Row> result1=  countFameAppearances.join(pitching,"playerID");
		result1.show();
		
//		Dataset<Row> result = DbReader.getSession().sql(sqlbuilder.toString());
//		result.show();
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
		file.mkdirs();
		file.createNewFile();
		System.out.println("saving to file"+file);
		FileWriter writer = new FileWriter(file);
		saveFile(avgSals,writer); 
				
	}
	public static void startProcessor(DbReader reader) {
	 	ERAProcessor proce = new ERAProcessor();
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

	private static final long serialVersionUID = 6319324370196471909L;
	static FileWriter writer = null;
 	@Override
	public void call(HallOfFame t) throws Exception {
		writer.write(t != null ? t.toString() : "null");
		writer.flush();
	}
}