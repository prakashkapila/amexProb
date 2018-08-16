package com.amex.vo;

import java.io.Serializable;
import java.text.NumberFormat;

import org.apache.spark.sql.Row;

public class Rankings implements Serializable {
	private static final NumberFormat FORMAT = NumberFormat.getInstance();
	
	private static final long serialVersionUID = 8518560273626772214L;
	private String teamID;
	private Integer  year, rank, atBats;
	
	public static String COLUMNS() {
		return "Team ID, Year, Rank, At Bats";
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb
		.append(teamID).append(",")
		.append(FORMAT.format(year)).append(",")
		.append(FORMAT.format(rank)).append(",")
		.append(FORMAT.format(atBats));
		return sb.toString();
	}
	public void apply(String... row) {
		teamID=row[0];
		year = Integer.valueOf(row[1]);
		rank=Integer.valueOf(row[2]);
		atBats=Integer.valueOf(row[3]);
	}
	
	public void apply(Row row)
	{
		teamID=row.getString(0);
		year = row.getInt(1);
		rank=row.getInt(2);
		atBats=row.getInt(3);
	}
	public String getTeamID() {
		return teamID;
	}

	public void setTeamID(String teamID) {
		this.teamID = teamID;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getRank() {
		return rank;
	}

	public void setRank(Integer rank) {
		this.rank = rank;
	}

	public Integer getAtBats() {
		return atBats;
	}

	public void setAtBats(Integer atBats) {
		this.atBats = atBats;
	}
}
