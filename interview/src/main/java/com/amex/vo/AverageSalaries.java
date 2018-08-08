package com.amex.vo;

import java.io.Serializable;
import java.text.NumberFormat;

import org.apache.spark.sql.Row;

public class AverageSalaries implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4347263813346835411L;
	String year, fielding, pitching;
	static final NumberFormat FORMAT = NumberFormat.getInstance();
	
	public static String COLUMNS() {
		return "Year,Fielders,Pitchers";
	}
	
	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	 

	public String getFielding() {
		return fielding;
	}

	public void setFielding(String fielding) {
		
		this.fielding = fielding;
	}

	public String getPitching() {
		return pitching;
	}

	public void setPitching(String pitching) {
		this.pitching = pitching;
	}
	
	private String getString(Object obj)
	{
		if(obj instanceof java.lang.Double)
		{
			return FORMAT.format(obj);
		}
		else if(obj instanceof java.lang.Integer)
		{
			return FORMAT.format(obj);
		}
		else
			return String.valueOf(obj);
	}
	
	public void apply(Row row)
	{
		setYear(getString(row.get(0)));
	 	setFielding(getString(row.get(1)));
	 	setPitching (getString(row.get(2)));
	}
	
	
	@Override
	public String toString() {
		return this.year+","+this.fielding+","+this.pitching;
	}
}
