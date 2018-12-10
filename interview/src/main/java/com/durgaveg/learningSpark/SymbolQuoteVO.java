package com.durgaveg.learningSpark;

import java.io.Serializable;
import java.util.Calendar;

import scala.collection.Iterator;

/**
 * Hello world!
 *
 */
public class SymbolQuoteVO implements Serializable 
{
	private static final long serialVersionUID = -1431027195343391925L;
	private String symbol;
	private double price;
	private String time;
	private double percent;
	private double changedBy;
	private double volume;
	private String company;
	private String currentStamp;
	
	public void setCurrentStamp(String str)
	{
		this.currentStamp+=str;
	}
	
	public void setCurrentStamp(String price,String date)
	{
		
		this.currentStamp+=(price+","+date);
	}
	
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {

		String vals[] = symbol.substring(symbol.indexOf("symbol")).split("=");
		this.symbol = vals[1].substring(0, vals[1].indexOf("class"));
		this.company=vals[4].substring(vals[4].indexOf(">")+1);
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(String string) {
		int start =0;
		start = string.contains("$")?string.indexOf("$")+1:string.indexOf(">")+1;
		string = string.substring(start,string.indexOf("</"));
		this.price = Double.valueOf(string);
	}
	public String getTime() {
		return time;
	}
	public void setTime() {
		Calendar time = Calendar.getInstance();
		this.time = new StringBuilder().
				append(time.get(Calendar.DATE))
				.append("-").append(time.get(Calendar.HOUR_OF_DAY))
				.append(":").append(time.get(Calendar.MINUTE))
				.toString();
	}
	public double getPercent() {
		return percent;
	}
	public void setPercent(double percent) {
		this.percent = percent;
	}
	public double getChangedBy() {
		return changedBy;
	}
	public void setChangedBy(String string) {
		string = string.substring(string.indexOf(">")+1,string.indexOf("</"));
		this.changedBy = Double.valueOf(string);
	}
	public double getVolume() {
		 return volume;
	}
	public void setVolume(double volume) {
	 		this.volume = volume;
	}
	public void setVolume(String string) {
		if(string != null && string.contains("<")) {
			string = string.substring(string.indexOf(">")+1,string.indexOf("</"));
		}
		string = string.replace(",", "");
		this.volume = Double.valueOf(string);
	}
	private void setPercent(String string) {
	 	string = string.substring(string.indexOf(">")+1,string.indexOf("</"));
		this.percent = Double.valueOf(string);	
	}
	public void init(Iterable<String> iterableStrings) {
		java.util.Iterator<String> it = iterableStrings.iterator();
		int i=0;
		String str="";
		setTime();
		do {
			str= it.next();
			if(checkString(str)) {
				switch(i++) {
				case 0:
				{
					setSymbol(str);
					break;
				}
				case 1:{
					setPrice(str);
					break;
				}

				case 2:{
					setChangedBy(str);
					break;
				}

				case 3:{
					setPercent(str);
					break;
				}

				case 4:{
					setVolume(str);
					break;
				}
				}
			}
		}while (it.hasNext());
	}

	
	private boolean checkString(String str) {
		boolean ret = false;
		if(str == null || str.trim().equals(""))
			return false;
		if(str.indexOf("num")>0 )
			return true;
		if(str.indexOf("symbol")>0)
			ret= true;
		return ret;
	}
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(symbol).append(",")
		.append(this.changedBy).append(",")
		.append(percent).append(",")
		.append(volume).append(",")
		.append(price).append(",")
		.append(time);

		return sb.toString();
	}
public String getMetaData() {
	
	return	new StringBuilder().append("symbol").append(",changedBy").append(",percent").append(",volume").append(",price").append(",time").toString();
		
	}
}
