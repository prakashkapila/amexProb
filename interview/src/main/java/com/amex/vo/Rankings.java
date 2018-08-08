package com.amex.vo;

import java.io.Serializable;

public class Rankings implements Serializable {

	private static final long serialVersionUID = 8518560273626772214L;
	Integer teamID, year, rank, atBats;

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
