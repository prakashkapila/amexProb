package com.amex.db;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import static com.amex.db.SparkSessionManager.INSTANCE;

public class DbReader {
	private static SQLContext sqlcontext;
	static {sqlcontext = new SQLContext(INSTANCE);}
	
 	public Dataset<Row> read(String table) {
	 	Dataset<Row> dataframe_mysql = sqlcontext.read().format("jdbc")
				.option("url", "jdbc:mysql://localhost:3306/lahman2016")
				.option("driver", "com.mysql.jdbc.Driver")
				//.option("driver", "com.mysql.cj.jdbc.Driver")
				.option("dbtable", table)
				.option("user", "user")
				.option("password", "pswd")
				.load();
		//dataframe_mysql.show();
	 	sqlcontext.clearCache();
	 	
		return dataframe_mysql;
	}
	
 	public static SparkSession getSession() {
 		return sqlcontext.sparkSession();
 	}
 	
	public static void main(String arf[])
	{
		DbReader reader = new DbReader();
		reader.read("appearances");
		reader.read("salaries");
		
	}
}
