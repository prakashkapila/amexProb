package com.amex.vo;

import java.io.Serializable;
import java.text.NumberFormat;

import org.apache.spark.sql.Row;

public class Rankings implements Serializable {
	private static final NumberFormat FORMAT = NumberFormat.getInstance();
	
	private static final long serialVersionUID = 8518560273626772214L;
	private String minTeamID;
	private Integer  year, minRank, minAtBats;
	
	public static String COLUMNS() {
		return "Team ID, Year, Rank, At Bats";
	}
	
	public String getMinTeamID() {
		return minTeamID;
	}

	public void setMinTeamID(String minTeamID) {
		this.minTeamID = minTeamID;
	}

 

	public Integer getMinRank() {
		return minRank;
	}

	public void setMinRank(Integer minRank) {
		this.minRank = minRank;
	}

	

	public Integer getMinAtBats() {
		return minAtBats;
	}

	public void setMinAtBats(Integer minAtBats) {
		this.minAtBats = minAtBats;
	}

	 

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb
		.append(minTeamID).append(",")
		.append(FORMAT.format(year)).append(",")
		.append(FORMAT.format(minRank)).append(",")
		.append(FORMAT.format(minAtBats));
		return sb.toString();
	}
	public void apply(Object... row) {
		minTeamID=(String)row[0];
		year = (Integer)row[1];
		minAtBats=(Integer)row[2];
		minRank=(Integer)row[3];
 	}
	
	public boolean equals(Object other)
	{
		boolean ret = false;
		if(other == null || (!(other instanceof Rankings)))
			return false;
		Rankings oth =(Rankings)other; 
		ret = oth.getAtBats().equals(getMinAtBats())
				&& oth.getTeamID().equalsIgnoreCase(getTeamID())
				&& oth.getYear().equals(getYear());
		return ret;
	}
	public void apply(Row row)
	{
		minTeamID=row.getString(1);
		year = row.getInt(0);
		minRank=row.getInt(3);
		minAtBats=row.getInt(2);
	}
	public String getTeamID() {
		return minTeamID;
	}

	public void setTeamID(String teamID) {
		this.minTeamID = teamID;
	}

	public Integer getYear() {
		return year;
	}

	public void setYear(Integer year) {
		this.year = year;
	}

	public Integer getRank() {
		return minRank;
	}

	public void setRank(Integer rank) {
		this.minRank = rank;
	}

	public Integer getAtBats() {
		return minAtBats;
	}

	public void setAtBats(Integer atBats) {
		this.minAtBats = atBats;
	}
}
