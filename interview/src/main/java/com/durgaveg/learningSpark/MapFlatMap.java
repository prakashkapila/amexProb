package com.durgaveg.learningSpark;


 
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class MapFlatMap implements Serializable {

	private static final long serialVersionUID = -2120652636520493737L;
	volatile int i =0;
	volatile int j = 10;
	public void add(JavaSparkContext sc) {
		JavaRDD<String> lines2RDD 	= sc.parallelize(Arrays.asList("Java World", "intersecting", "Hadoop and MapReduce", "Pig and Hive"));
		JavaRDD<String> linesRDD 	= sc.parallelize(Arrays.asList("hello world Spark and Scala in real world intersecting world hello"));
		JavaRDD<String> lines3RDD 	= sc.parallelize(Arrays.asList("Spark and Scala in real world intersecting"));
	
		
		JavaRDD<String> flatMapPairsRDD 	= linesRDD.flatMap(new MyFlatMap2());
		JavaRDD<String> flatMap3RDD    = lines3RDD.flatMap(new MyFlatMap2());
		
		System.out.println("FlatMap:" 	+ flatMapPairsRDD.take(20) );
		
		JavaPairRDD<Integer, String> pairs = flatMapPairsRDD.mapToPair(s->new Tuple2<Integer, String>(++i, s.toUpperCase()));
		JavaPairRDD<Integer, String> pairs3RDD = flatMap3RDD.mapToPair(s->new Tuple2<Integer, String>(++j, s.toUpperCase()));
		
		JavaPairRDD<String, String> pairsStr = pairs.mapToPair(s->new Tuple2<String, String>(s._2, String.valueOf(s._1) ) );
		
		JavaPairRDD<String, String> pairs3Str = pairs3RDD.mapToPair(s->new Tuple2<String, String>(s._2, String.valueOf(s._1) ) );
		
		pairsStr.collectAsMap().forEach(new BiConsumer<String, String>(){
			public void accept(String u, String t){
				System.out.println("PairsStr Join word is" + u + "\t the value is " + t );
			}
		});
		
		pairs3Str.collectAsMap().forEach(new BiConsumer<String, String>(){
			public void accept(String u, String t){
				System.out.println("Pairs3Str Join is" + u + "\t the value is " + t );
			}
		});
		
//		pairs.collectAsMap().forEach(new BiConsumer<Integer, String>(){
//			public void accept(Integer u, String t){
//				System.out.println("Pairs Join is" + t + "\t the value is " + u );
//			}
//		});
//		
//		pairs3RDD.collectAsMap().forEach(new BiConsumer<Integer, String>(){
//			public void accept(Integer u, String t){
//				System.out.println("Pairs3RDD Join is" + t + "\t the value is " + u );
//			}
//		});
		
		(pairs.join(pairs3RDD)).collectAsMap().forEach(new BiConsumer<Integer, Tuple2<String,String>>(){
			public void accept(Integer t, Tuple2<String, String> u){
				System.out.println("Join is " + t + "\t the value is " + u._1()+ " \t " + u._2() );
			}
		});
				
		JavaRDD<String> flatMapRDD 	= linesRDD.flatMap(new MyFlatMap2());
		JavaRDD<String> mapRDD 		= lines2RDD.map(new MyMap());
		
		JavaRDD<String> map2RDD 		= lines3RDD.map(new MyMap());
//		JavaRDD<String> flatMap3RDD = lines3RDD.flatMap(new MyFlatMap2());
		
//		System.out.println("FlatMap:" 	+ flatMapRDD.take(20) );
//		System.out.println("Map:" 		+  mapRDD.take(20) );
//	
//		System.out.println("Union:" + (flatMapRDD.union(mapRDD)).take(20));
//		System.out.println("Subtract" + (flatMapRDD.subtract(flatMap3RDD)).take(20));
//		System.out.println("Subtract2" + (flatMap3RDD.subtract(flatMapRDD)).take(20));		
//		System.out.println("FlatMap Intersection" + (flatMapRDD.intersection(flatMap3RDD)).take(20));
//		
//		System.out.println("Map Intersection" + (mapRDD.intersection(map2RDD)).take(20));
//		System.out.println("Map Union" + (mapRDD.union(map2RDD)).take(20));
//		System.out.println("Map Subtract" + (mapRDD.subtract(map2RDD)).take(20));		
//		System.out.println("Map (map2-map) Subtract" + (map2RDD.subtract(mapRDD)).take(20));
		
		
	}
}

class MyMap implements Function<String, String>{
	private static final long serialVersionUID = -7597951529157624187L;

	@Override
	public String call(String arg0) throws Exception {
		return arg0+"?";
	}			
}

class MyFlatMap2 implements FlatMapFunction<String,String> {
	private static final long serialVersionUID = 5941028234880857233L;

	public Iterator<String> call(String line) {
		return Arrays.asList(line.split(" ")).iterator();
	}
}

