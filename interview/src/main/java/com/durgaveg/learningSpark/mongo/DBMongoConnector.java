package com.durgaveg.learningSpark.mongo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import org.bson.Document; 
import java.util.*;

import com.durgaveg.learningSpark.Stock;
import com.durgaveg.learningSpark.SymbolQuoteVO;
import com.google.gson.Gson;
import com.mongodb.spark.MongoSpark;

import java.io.Serializable;
public class DBMongoConnector implements Serializable{

	private static final long serialVersionUID = -3459656215445752239L;
SparkSession session;

 private void initSession() {
	  session = session != null ? session :
		  SparkSession.builder().master("local[4]").appName("MongoConnectorStock")
		  .config("spark.app.id", "MongoConnectorStock")
		  .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.Stocks")
	      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.Stocks")
	     .getOrCreate();
		
 }
 
 private List<Stock> getMockStockList(){
	 List<Stock> ret = new ArrayList<Stock>();
	 String[] names = new String[]{"IBM","MS","GS","HDP","BAC","JPM","AMAT","NVDA"};
	 for (int i=0;i<20;i++)
	 {
		 Stock stock = new Stock();
		 if( i%2 ==0) {
			 stock.setExchange("NYSE");
		 }else {
			 stock.setExchange("NASDAQ");
		 }
		 stock.setName(names[i%7]);
		 stock.setPrice((i+1) * Math.random()*100);
		 stock.setPriceChange(.02);
		 ret.add(stock);
 	 }
	 return ret;
 }
 
 public  void insertRecords(JavaRDD<SymbolQuoteVO> elements,SparkSession thisSession) {
	  Dataset<Row> rows = thisSession.createDataFrame(elements,  SymbolQuoteVO.class);
	  Dataset<SymbolQuoteVO> stocklist= rows.as(Encoders.bean(SymbolQuoteVO.class));
	  MongoSpark.write(stocklist).option("collection", "StockQuotes").mode(SaveMode.Overwrite);
 }
 public void testConnection() {
 	 initSession();
	 Dataset<Stock> stocklist = session.createDataset(getMockStockList(),Encoders.bean(Stock.class));
	 JavaRDD<Document> doc  = stocklist.javaRDD().map(new Function<Stock,Document>(){
		 private static final long serialVersionUID = 1L;
 		@Override
		public Document call(Stock v1) throws Exception {
 			Gson gson = new Gson();
 			String json = gson.toJson(v1);
 			System.out.println(json);
 		 	Document doc = Document.parse(json);
			return doc;
		}}); 
	 MongoSpark.write(stocklist).option("collection", ""+ "Stocks").mode("overwrite").save();

	//  MongoSpark.save(doc);
 }
 public static void main(String args[])
 {
	 DBMongoConnector connector = new DBMongoConnector();
	 connector.testConnection();
 }
}

