package com.amex.vo;

import java.io.Serializable;

import org.apache.spark.sql.Row;

public class HallOfFame implements Serializable {
	
	private static final long serialVersionUID = -2700316956494613227L;
	private String player;
	private Double era;
	private Integer appearnaces;
	private Integer inductionYear;

	
	 
	public void apply(Row row)
	{
		setPlayer(row.getString(0));
	 	setInductionYear(row.getInt(2));
	 	setAppearnaces(Long.valueOf(row.get(3).toString()).intValue());
	 	setEra(row.getDouble(4));
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(); 
		return sb.append(this.player).append(",")
				.append(String.valueOf(this.era)).append(",")
				.append(this.appearnaces).append(",")
				.append(this.inductionYear)
				.toString();
	}
	
	public String getPlayer() {
		return player;
	}

	public void setPlayer(String player) {
		this.player = player;
	}

	public Double getEra() {
		return era;
	}

	public void setEra(Double era) {
		this.era = era;
	}

	public Integer getAppearnaces() {
		return appearnaces;
	}

	public void setAppearnaces(Integer appearnaces) {
		this.appearnaces = appearnaces;
	}

	public int getInductionYear() {
		return inductionYear;
	}

	public void setInductionYear(int inductionYear) {
		this.inductionYear = inductionYear;
	}
	public static String COLUMNS() {
		return "Player, ERA, # All Star Appearances, Hall of Fame Induction Year";
	}
}
