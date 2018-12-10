package com.durgaveg.learningSpark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


interface Correlations{
	void doPearsonCorrelation();
	void doSpearmansCorrelation();
}



public class MyCorrelation implements Correlations{
	
	SparkSession jsc;
	MyCorrelation(SparkSession ses){
		this.jsc = ses;
	}
	private Dataset<SymbolQuoteVO> prepareStockData(){
		Dataset<SymbolQuoteVO> ret = null;
		List<SymbolQuoteVO> values = new ArrayList<SymbolQuoteVO>();
		for(int i=0;i<25;i++) {
			 SymbolQuoteVO anew = new SymbolQuoteVO();
			values.add(anew);
		}
		return ret;
	}
	private Dataset<Row> prepareData() {
		Dataset<Row> ret = null;
		List<Row> data = getData();
		StructType schema = getSchema();
		ret = jsc.createDataFrame(data, schema);
		return ret;
	}
	private StructType getSchema() {
		StructType schema = new StructType(new StructField[]{
				  new StructField("features", new VectorUDT(), false, Metadata.empty()),
				});
		return schema;
	}
	Random rand = new Random(10);
	
	private Vector getVectorValues(int i) {
			Vector v = null;
			int j = 100;
//		if(i%2==0)
// 		{
//			v=Vectors.sparse(4, new int[]{0, 3}, new double[]{i,i++});
//		}
//		else
//		{
			v=Vectors.dense(i,Math.pow(++i,j),Math.pow(++i,j),Math.pow(++i,j));
//		}
	//	System.out.println(v.asBreeze());
		System.out.println(v);
		return v;
	}
	
	private List<Row> getData() {
		List<Row> rows = new ArrayList<Row>();
		for(int i=0;i<10;i++)
		{
			rows.add(RowFactory.create(getVectorValues(i)));
		}
		return rows;
	}
	@Override
	public void doPearsonCorrelation() {
		// TODO Auto-generated method stub
		Dataset<Row> df = prepareData();
		
		Dataset<Row> corr = Correlation.corr(df, "features");
		corr.show(5);
		System.out.println("Shown correlation");
//		corr.collectAsList().forEach(new Consumer<Row>() {
//			@Override
//			public void accept(Row t) {
//				// TODO Auto-generated method stub
//		//		System.out.println(t.mkString());
//				for(int i=0;i<t.length();i++) {
//					System.out.println("Field Name" + t.get(i));
//				}
//				
//			}});
		Row row = corr.head();
		System.out.println("Pearson correlation matrix:\n" + row.mkString());

		System.out.println("End Pearson correlation matrix count: " + corr.count());
// 		Row r1 = Correlation.corr(df, "features").head();
//		System.out.println("Pearson correlation matrix:  " + r1.get(0).toString());
 			}

	@Override
	public void doSpearmansCorrelation() {
		// TODO Auto-generated method stub
		Dataset df = prepareData();
		Row r2 = Correlation.corr(df, "features", "spearman").head();
		System.out.println("Spearman correlation matrix:\n" + r2.get(0).toString());

	}
	
}
