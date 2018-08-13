package com.amex.processors;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import com.amex.db.DbReader;

public class RankingsProcessor {

	public void startProcess(DbReader reader) throws IOException {
		Dataset<Row> teams = reader.read("teams");
		functions.dense_rank();
		Dataset<Row> bats = reader.read("teams");
		
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

}
