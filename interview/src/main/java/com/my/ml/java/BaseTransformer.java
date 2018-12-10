package com.my.ml.java;

import java.util.UUID;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public abstract class BaseTransformer extends Transformer{
	String uid = UUID.fromString(String.valueOf(System.currentTimeMillis())).toString();
	
	@Override
	public String uid() {
 		return null;
	}

	@Override
	public Transformer copy(ParamMap arg0) {
		// TODO Auto-generated method stub
		ParamMap map = super.extractParamMap();
		 map.$plus$plus(arg0);
		 RowIdGenerator gen = new RowIdGenerator();
		 gen.uid=uid;
		 gen.extractParamMap().$plus$plus(map);
		 return gen;
	}
 
	@Override
	public StructType transformSchema(StructType arg0) {
		// TODO Auto-generated method stub
		return null;
	}
}
