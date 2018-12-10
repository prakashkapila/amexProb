package com.durgaveg.learningSpark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2; 
 
public class WordCount {
 JavaSparkContext session = null;
	

 	
	public void findCount(JavaRDD<String> logData,String... data) {
		for(String datum:data) {
		long numAs = logData.filter(var -> var.toUpperCase().contains(datum.toUpperCase())).count();
 		System.out.println("Lines with a: " + numAs );
		}
	}
	
 	public JavaRDD<String> readInput(String filpath)
 	{
 		JavaRDD<String> ret = session.textFile(filpath);
 		return ret;
 	}
 	
 	private void printSample(JavaRDD<String> allLines)
 	{
 		allLines.take(5).forEach(new Consumer<String>() {
			public void accept(String t)
			{
				System.out.println("The sentence is "+t);
			}
		});
  	}
 	public void countWords(JavaRDD<String> lines)
 	{
 		JavaRDD<String> lineLengths =lines.flatMap(s->Arrays.asList(s.split(" ")).iterator());
 		// javascript xsd = "simple" => type of xsd becomes string.
 		// ysd = 25; => ysd is a integer.
 		//
 		JavaPairRDD<String,Integer> allWordswithOne = 
 							lineLengths.mapToPair(s->new Tuple2<String,Integer>(s.toUpperCase().trim(),1)) // return type should be string and int pair or tuple.
 							.reduceByKey((a,b)->a+b);
 		//allWordswithOne.
 		allWordswithOne.collectAsMap().forEach(new BiConsumer<String,Integer>(){
  			public void accept(String t, Integer u) {
				System.out.println("The word is "+t+"\t the value is"+u);
			}});
 	}
 	
 	public void wordCount(JavaRDD<String> allLines)
 	{
 		JavaRDD<String> allWords = allLines.flatMap(new MyFlatMap());
 		printSample(allLines);
 	 	findCount(allLines,new String[]{"the","prince","oil"});
 		
 		JavaPairRDD<String,Integer> allWordswithOne = allWords.mapToPair(new IntegerMap());
 		/*
 		 * Initial
 		 * prince,1
 		 * Prince,1
 		 * prince,1
 		 * Prince,1
 		 * prince,1
 		 * 
 		 *In the method 1.(prince,1) ,1.(prince,1),1.(prince,1)
 		 * prince(1,1) => (prince,2),1.(prince,1)
 		 * 1.(prince,1) => (prince,3)
 		 * (Prince,2)
 		 * */
  		JavaPairRDD<String,Integer>  wordCount = allWordswithOne.reduceByKey(new CountMap());
  		
  		
  		Map normal = wordCount.collectAsMap();
  		
  		normal.forEach(new BiConsumer<String,Integer>(){
  			public void accept(String t, Integer u) {
				System.out.println("The word is "+t+"\t the value is"+u);
			}});
 	}
}
class IntegerMap implements PairFunction<String,String,Integer>{
 	public Tuple2<String, Integer> call(String t) throws Exception {
	 	return new Tuple2<String,Integer>(t,1);
	}
 }
class CountMap implements Function2<Integer,Integer,Integer>
{
	 private static final long serialVersionUID = 4028817627227565662L;
 	public Integer call(Integer v1, Integer v2) throws Exception {
 		return v1+v2;
	}
}

class MyFlatMap implements FlatMapFunction<String,String>{
 		private static final long serialVersionUID = 3106315431445832477L;
 	//inp:It took decades to develop Dubai into a tourist
 	// output {It,took,decades,to,develop,Dubai,into,a,tourist}
 	public Iterator<String> call(String t) throws Exception {
		String [] allwords = t.split(" ");
		return Arrays.asList(allwords).iterator();
	}
}