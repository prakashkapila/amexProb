package com.amex.db;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import java.io.Serializable;
import com.amex.vo.input.Appearances;
import com.amex.vo.input.Salaries;

public class SalaryRowConvertor implements Serializable{
 	 
	/**
	 * 
	 */
	private static final long serialVersionUID = -1576761844449424145L;

	public static Dataset<Salaries> convert(Dataset<Row> salRow)
	{
		Dataset<Salaries> appears= salRow.map(new Converter(), Encoders.bean(Salaries.class));
		return appears; 
	}
}

class Converter implements MapFunction<Row,Salaries>{

	 	private static final long serialVersionUID = -7397950735008253203L;
		 	public Salaries call(Row value) throws Exception {
		 		Salaries ret = new Salaries();
			for(int i=0;i<value.size();i++)
			{
				ret.apply(value.get(i),i);
			}
		 	return ret;
	 }
}