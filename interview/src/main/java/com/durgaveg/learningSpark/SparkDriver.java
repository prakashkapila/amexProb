package com.durgaveg.learningSpark;

import java.io.Serializable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkDriver implements Serializable{
 private static final long serialVersionUID = 1381321917005361704L;
	JavaSparkContext jsc;
// spark submit --master yarn SparkDriver.
	public JavaSparkContext getSession() {
			if (jsc == null) {
				 SparkConf conf  = new SparkConf(true).setMaster("local[4]")
						.setAppName("SmartSparkLearner")
						;
				 jsc = new JavaSparkContext(conf);
		 	}
			return jsc;
		}
	public void sumbitCorrelation() {
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf conf  = new SparkConf(true).setMaster("local[4]")
				.setAppName("SmartSparkLearner")
				;
		SparkSession session = SparkSession.builder().config(conf).getOrCreate();
		MyCorrelation corr = new MyCorrelation(session);
		corr.doPearsonCorrelation();
		corr.doSpearmansCorrelation();
	}
	public void submitApplication() {
		WordCount count = new WordCount();
		jsc=jsc==null?getSession():jsc;
		count.session = jsc;
		count.countWords(count.readInput("C:\\docs\\quotes\\newsarticle.txt"));
	}
	public void submitGraphTraversal() {
		 Graph graph = new Graph();
		 jsc=jsc==null?getSession():jsc;
		 graph.jsc=this.jsc;
		 graph.run();
	}
	public void submitMapAndJoin() {
		MapAndFlatMap mappers = new MapAndFlatMap();
		jsc=jsc==null?getSession():jsc;
		mappers.jsc = this.jsc;
		mappers.run();
	}
	public static void main(String arg[])
	{
		SparkDriver driver = new SparkDriver();
	  	//driver.referencePassing();		
		//driver.submitApplication();
		//driver.submitGraphTraversal();
		//driver.submitMapAndJoin();
		driver.sumbitCorrelation();
		//Recommenders.doALS(driver.getSession());
	}
}
class Something{
	String s="";
	public void addS(String ars)
	{
		s=s+ars;
	}
}