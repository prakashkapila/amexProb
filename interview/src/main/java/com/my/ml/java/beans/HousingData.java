package com.my.ml.java.beans;

import java.io.Serializable;

import org.apache.spark.sql.Row; 

public class HousingData implements Serializable
{
	private static final long serialVersionUID = -2231872979240422699L;
	double longitude,latitude,housing_median_age,median_house_value;
	double median_income=0.0;
	int total_rooms,total_bedrooms, population,households;
	String ocean_proximity;
	@Override
	public String toString() {
		return "longitude="+longitude+"\tlatitude="+latitude+"\thousing_median_age="+housing_median_age+"\tmedian_income="+median_income+"median_house_value="+median_house_value;
	}
	public static HousingData fill(String row)
	{
		HousingData ret = new HousingData();
		String cols[] = row.split("\\,");
		for(int i=0;i<cols.length;i++)
		{
			fill(ret,i,cols[i]);
		}
		return ret;
	}
	public static HousingData fill(Row row)
	{
		HousingData ret = new HousingData();
		for(int i=0;i<row.size();i++)
		{
			fill(ret,i,row.getString(i));
		}
		return ret;
	}

	public static void fill(HousingData ret ,int i,String row)
	{
		switch(i) {
		case 0:{
			ret.longitude = row != null ? Double.parseDouble(row):0.0;
			break;

		}
		case 1:{
			ret.latitude=row != null ? Double.parseDouble(row):0.0;
			break;
		}
		case 2:{
			ret.housing_median_age=row != null ? Integer.parseInt(row):0;
			break;
		}
		case 3:{
			ret.total_rooms=row != null ? Integer.parseInt(row):0;
			break;
		}
		case 4:{
			ret.total_bedrooms =row != null ? Integer.parseInt(row):0;
			break;
		}
		case 5:{
			ret.population =row != null ? Integer.parseInt(row):0;
			break;
		}
		case 6:{
			ret.households=row != null ? Integer.parseInt(row):0;
			break;
		}
		case 7:{
			ret.median_income=row != null ? Double.parseDouble(row):0.0;
			break;
		}
		case 8:{
			ret.median_house_value=row != null ? Double.parseDouble(row):0.0;
			break;
		}
		case 9:{ 
			ret.ocean_proximity = row;
			break;
		}

		} 
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getHousing_median_age() {
		return housing_median_age;
	}
	public void setHousing_median_age(double housing_median_age) {
		this.housing_median_age = housing_median_age;
	}
	public double getMedian_house_value() {
		return median_house_value;
	}
	public void setMedian_house_value(double median_house_value) {
		this.median_house_value = median_house_value;
	}
	public double getMedian_income() {
		return median_income;
	}
	public void setMedian_income(double median_income) {
		this.median_income = median_income;
	}
	public int getTotal_rooms() {
		return total_rooms;
	}
	public void setTotal_rooms(int total_rooms) {
		this.total_rooms = total_rooms;
	}
	public int getTotal_bedrooms() {
		return total_bedrooms;
	}
	public void setTotal_bedrooms(int total_bedrooms) {
		this.total_bedrooms = total_bedrooms;
	}
	public int getPopulation() {
		return population;
	}
	public void setPopulation(int population) {
		this.population = population;
	}
	public int getHouseholds() {
		return households;
	}
	public void setHouseholds(int households) {
		this.households = households;
	}
	public String getOcean_proximity() {
		return ocean_proximity;
	}
	public void setOcean_proximity(String ocean_proximity) {
		this.ocean_proximity = ocean_proximity;
	}

}