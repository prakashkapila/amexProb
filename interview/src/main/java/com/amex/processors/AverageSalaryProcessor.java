package com.amex.processors;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.amex.db.AppearancesRowConvertor;
import com.amex.db.DbReader;
import com.amex.db.SalaryRowConvertor;
import com.amex.vo.AverageSalaries;
import com.amex.vo.input.Appearances;
import com.amex.vo.input.Salaries;

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

	public Dataset<AverageSalaries> getAvgSalaries()  {
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
 		Dataset result = rows.join(rows2, "yearID");
 		result.show();
		System.out.println("Showing salaries both pitchers exclusive  & fielders");
	//	salaries.show();
		return null; 
	}

	public static void main(String arg[]) {
		AverageSalaryProcessor processor = new AverageSalaryProcessor();
		 	processor.getAvgSalaries();
		
	}
}
