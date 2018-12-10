package com.my.ml.java.helper;

import org.apache.spark.api.java.function.Function;

import com.my.ml.java.beans.HousingData;

public class HousingMap implements Function<String,HousingData>{
	private static final long serialVersionUID = 8888682305899125389L;

	@Override
	public HousingData call(String v1) throws Exception {
		String[] values = v1.split(",");
		HousingData data = new HousingData();
		int i=0;
		do {
			HousingData.fill(data,i, values[i]);
			i++;
		}while( i < values.length);
		return data;
	}
}