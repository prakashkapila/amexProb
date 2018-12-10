package com.my.ml.java.helper;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import com.my.ml.java.beans.*;

public class HousingMapRow implements MapFunction<Row,HousingData>{
	private static final long serialVersionUID = -5109635169941310841L;
	private boolean isDigit(String str) {
		char [] chars = str.toCharArray();
		for(int i=0;i< chars.length;i++)
		{
			if(Character.isDigit(chars[i]) || chars[i]=='-' || chars[i]=='.')continue;
			else
				return false;
		}
		return true;
	}
@Override
public HousingData call(Row value) throws Exception {
		HousingData data = new HousingData();
		int i=0;
	 	 do {
	 		data.fill(data,i, value.getString(i));
		 			i++;
			}while(i<value.size());
	 return data;
}
}