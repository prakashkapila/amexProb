package com.my.ml.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class RowIdGenerator extends BaseTransformer {
	private static final long serialVersionUID = -666337514996266549L;
 	
  @Override
	public Dataset<Row> transform(Dataset<?> arg0) {
		Dataset<Row> rowIdCatg = arg0.withColumn("rowId",functions.monotonicallyIncreasingId());
		return rowIdCatg;
	}
 
}
