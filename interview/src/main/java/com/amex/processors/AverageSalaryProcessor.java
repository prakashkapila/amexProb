package com.amex.processors;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.amex.db.AppearancesRowConvertor;
import com.amex.db.DbReader;
import com.amex.db.SalaryRowConvertor;
import com.amex.vo.AverageSalaries;
import com.amex.vo.input.Appearances;
import com.amex.vo.input.Salaries;
import com.mongodb.spark.sql.fieldTypes.api.java.Timestamp;

import scala.tools.nsc.interpreter.Results;

public class AverageSalaryProcessor {

	public Dataset<Appearances> fetchAppearances() {
		DbReader reader = new DbReader();
		Dataset<Appearances> ret = AppearancesRowConvertor.convert(reader.read("appearances"));
		return ret;
	}

	public Dataset<Row> getPitchersOnly(Dataset<Row> allPlayers) {
		// get all columns g1b, g2b, g3b, gss or gp
		return allPlayers.where(new Column("G_p").$greater("0").and
				(new Column("G_1b").equalTo("0")
				.and(new Column("G_2b").equalTo("0")
				.and(new Column("G_3b").equalTo("0"))
				.and(new Column("G_ss").equalTo("0"))
					)
				)
				);
	}

	public Dataset<Row> getInfieldersOnly(Dataset<Row> allPlayers) {
		// get all columns g1b, g2b, g3b, gss or gp
		allPlayers.createOrReplaceGlobalTempView("appearances");
		return allPlayers.where
				(new Column("G_1b").$greater("0")
				.or(new Column("G_2b").$greater("0")
				.or(new Column("G_3b").$greater("0"))
				.or(new Column("G_ss").$greater("0"))
					)
				)
				;
	}

	private Dataset<AverageSalaries> getAvgSalaries()  {
		Dataset<Row> salaries = null;
		DbReader reader = new DbReader();
		Dataset<Row> ret1 = reader.read("appearances");
	 	Dataset<Row> pitchersOnly = getPitchersOnly(ret1);
		pitchersOnly.createOrReplaceTempView("pitchers");
	 	Dataset<Row> fieldersOnly = getInfieldersOnly(ret1);
		fieldersOnly.createOrReplaceTempView("fielders");
	  	salaries = reader.read("salaries");
	 	salaries.createOrReplaceTempView("salaries");
	 	
	 	salaries.show();
	 	salaries = salaries.withColumnRenamed("playerId", "salaryPlayerId");
	
		SparkSession sess = fieldersOnly.sparkSession();
		Dataset<Row> rows = sess.sql("select salaries.*,fielders.playerID"
				+ " from salaries,fielders "
				+ "where salaries.playerID =fielders.playerID ");
				//+ "group by salaries.salary");
		rows =rows.groupBy("yearID").avg("salary");
		rows=rows.withColumnRenamed("avg(salary)", "Fielding");
		rows.show();
		
		Dataset<Row> rows2 = sess.sql("select salaries.*,pitchers.playerID"
				+ " from salaries,pitchers "
				+ "where salaries.playerID =pitchers.playerID ");
				//+ "group by salaries.salary");
		rows2 =rows2.groupBy("yearID").avg("salary");
		rows2=rows2.withColumnRenamed("avg(salary)", "Pitcher");
		rows2.show(); 
 		Dataset<Row> result = rows.join(rows2, "yearID");
 		result.show();
		System.out.println("Showing salaries both pitchers exclusive  & fielders");
		Dataset<AverageSalaries> aveSals = 
				result.map(new MapFunction<Row, AverageSalaries>() {
			private static final long serialVersionUID = 8909200548043475227L;
			@Override
			public AverageSalaries call(Row value) throws Exception {
				AverageSalaries sal = new AverageSalaries();
				sal.apply(value);
				return sal;
			}
			
		}, Encoders.bean(AverageSalaries.class));
		return aveSals; 
	}

	private void saveFile(Dataset<AverageSalaries> avgSals,final FileWriter writer) throws IOException
	{
 		writer.write(AverageSalaries.COLUMNS());
		writer.flush();
		avgSals.foreach(x->writer.write(x.toString()));
	}
	
	private void saveFile(Dataset<AverageSalaries> avgSals) throws IOException
	{
		File file = new File("/AvgSalaries"+System.currentTimeMillis()+".csv");
		file.mkdirs();
		file.createNewFile();
		System.out.println("saving to file"+file);
		FileWriter writer = new FileWriter(file);
		saveFile(avgSals,writer); 
				
	}
	
	public void startProcess() {
		try {
			saveFile(getAvgSalaries());
		} catch (IOException e) {
			 e.printStackTrace();
		}
	}
	public static void startProcessor() {
		AverageSalaryProcessor processor = new AverageSalaryProcessor();
	 	processor.startProcess();

	}
	public static void main(String arg[]) {
		startProcessor();
	}
}
