package com.durgaveg.learningSpark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
/**Generic class for union add subtract join on Maps and flatMaps**/
public class MapAndFlatMap implements Serializable{ 
 	private static final long serialVersionUID = -3837425124602478485L;
	JavaSparkContext jsc;
	
 	public void union(JavaRDD<String> map1,JavaRDD<String> map2,String comment)
 	{
 		map1.union(map2).collect().forEach(new Consumer<Object>() {
 			@Override
			public void accept(Object t) {
 				System.out.println("OP:"+comment+"\t"+t);
 			}});
 		
 	}
	public void subtract(JavaRDD<String> map1,JavaRDD<String> map2,String comment)
 	{
 		map1.subtract(map2).collect().forEach(new Consumer<String>() {
 			@Override
			public void accept(String t) {
				System.out.println("OP:"+comment+t);
			}});
 	}
 	public void intersection(JavaRDD<String> map1,JavaRDD<String> map2,String comment) {
 		List<String> result = map1.intersection(map2).collect();
 		//System.out.println(result);
 		result.forEach(new Consumer<String>() {
 		@Override
			public void accept(String t) {
				System.out.println(comment+t);
			}});
 	}
 	private void joins(JavaPairRDD<String,String> map1,JavaPairRDD<String,String> map2)
 	{map1.join(map2).collect().forEach(new Consumer<Tuple2<String,Tuple2<String,String>>>(){
		 	@Override
		public void accept(Tuple2<String, Tuple2<String, String>> t) {
			System.out.println("Key Value"+t._1());
			System.out.println("Mapped values <"+t._2()+">");
		}});}
 	public void join(JavaPairRDD<String,String> map1,JavaPairRDD<String,String> map2) {
 		System.out.println("join without the reduce on the String value pairs");
 		joins(map1,map2);
 		map1 = map1.reduceByKey((a,b)->a+" "+b);
 		map2 = map2.reduceByKey((a,b)->a+" "+b);		
 		System.out.println("join with the reduce on the String value pairs");
 		joins(map1,map2);
 	}
 	Integer value=1;
	Integer avalue=10;
	 	
 	public void run() {
 		String line1[] = {"This is real","Spark and scala Implementation","Of Map and flat map"};
 	 	String line2[] = {"This isent real","Sparks and scalas Implementation","Of Maps and flats map"};
 	 	
 		JavaRDD<String> strs = jsc.parallelize(Arrays.asList(line1));
 		JavaRDD<String> str2 = jsc.parallelize(Arrays.asList(line2));
 		JavaRDD<String> map1 = strs.flatMap(s->Arrays.asList(s.split(" ")).iterator());
 		JavaRDD<String> map2 = str2.flatMap(s->Arrays.asList(s.split(" ")).iterator());
 		union(map1,map2,"Union operation between map1 & map2");
 		union(map1,map2,"Union operation between map2 & map1");
 		subtract(map1,map2,"Subtraction (map1,map2");
 		subtract(map2,map1,"Subtraction (map2,map1");
 		intersection(map2,map1,"Intersectin (map2,map1");
 		intersection(map1,map2,"Intersectin (map1,map2");
 		
// 	 JavaPairRDD<String,String> str2KeyPair = map1.mapToPair(s->{
// 		final long serialVersionUID1 = -383742512460247848L;
// 	 	Tuple2<String,String> ret = new Tuple2<String, String>(s.toUpperCase(),String.valueOf(++value));
// 		return ret;
// 	 });
 		JavaPairRDD<String,String> str2KeyPair = map2.mapToPair(new MyPairFunction(value));
 		JavaPairRDD<String,String> strKeyPair = map1.mapToPair(new MyPairFunction(avalue));
 		
//  		
 	 //	JavaPairRDD<String,String> strKeyPair = map2.mapToPair(s->new Tuple2<String,String>(s.toUpperCase(),String.valueOf(++avalue)));
 		join(str2KeyPair,strKeyPair);
 		join(strKeyPair,str2KeyPair);
 		
 	}
 	
	 
}
class MyPairFunction implements PairFunction<String,String,String>{
	private static final long serialVersionUID = -71370096495248229L;
	volatile int i;
	public MyPairFunction(int i)
	{this.i = i;}
	@Override
	public Tuple2<String, String> call(String t) throws Exception {
		 return new Tuple2<String,String>(t,String.valueOf(i++));
	}
}