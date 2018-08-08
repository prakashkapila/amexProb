package com.amex.vo;

import java.io.Serializable;
import java.text.NumberFormat;

import org.apache.spark.sql.Row;

public class Rankings implements Serializable {
	private static final NumberFormat FORMAT = NumberFormat.getInstance();
	
	private static final long serialVersionUID = 8518560273626772214L;
	
	private Integer teamID, year, rank, atBats;
	
	public static String getCols() {
		return "Team ID, Year, Rank, At Bats";
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb
		.append(FORMAT.format(teamID)).append(",")
		.append(FORMAT.format(year)).append(",")
		.append(FORMAT.format(rank)).append(",")
		.append(FORMAT.format(atBats));
		return sb.toString();
	}
	public void apply(Row row)
	{
		teamID=row.getInt(0);
		year = row.getInt(1);
		rank=row.getInt(2);
		atBats=row.getInt(3);
	}
	public Integer getTeamID() {
		return teamID;
	}

	public void setTeamID(Integer teamID) {
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
