package com.amex.db;
import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import com.amex.vo.input.Appearances;
 
public class AppearancesRowConvertor implements Serializable {
   private static final long serialVersionUID = 6441374008478162635L;

	public static Dataset<Appearances> convert(Dataset<Row> appearances)
	{
		Dataset<Appearances> appears= appearances.map(new MapFunction<Row,Appearances>(){
 		 	private static final long serialVersionUID = -7397950735008253203L;
 	public Appearances call(Row value) throws Exception {
				Appearances ret = new Appearances();
				for(int i=0;i<value.size();i++)
				{
					ret.apply(value.get(i),i);
				}
			 	return ret;
			}
			}, Encoders.bean(Appearances.class));
		return appears; 
	}
}
