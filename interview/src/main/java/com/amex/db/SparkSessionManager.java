package com.amex.db;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSessionManager {
	public static final SparkSession INSTANCE ;
	static {
		INSTANCE = init();
	}
	
	private SparkSessionManager() {
		
	}
	
	private static SparkSession  init() {
			SparkConf config = new SparkConf(true)
				.set("spark.scheduler.mode", "FAIR")
				.set("spark.driver.memory", "5g")
				.set("spark.driver.cores", "4")
				.set("spark.executor.memory", "1g")// for giving all tasks a chance to execute.
			    ;
		return SparkSession.builder()
				 .master("local")
				.appName("MySqlSession")
				.config(config)
				.getOrCreate();
		 
	}

}
