package com.durgaveg.learningSpark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.durgaveg.learningSpark.ml.predictions.ALSRecommender;

//import org.apache.spark.sql.SparkSession;
import java.io.Serializable;
public class Recommenders {

	public static void doALS(JavaSparkContext session) {
		ALSRecommender als = new ALSRecommender(session);
		als.performALS();
	}

}

