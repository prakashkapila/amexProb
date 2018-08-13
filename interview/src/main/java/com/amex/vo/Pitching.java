package com.amex.vo;
import java.io.Serializable;
import java.text.NumberFormat;

import org.apache.spark.sql.Row;

public class Pitching implements Serializable{
	 
	private static final long serialVersionUID = 3615110930547267695L;
	
	private static final NumberFormat FORMAT = NumberFormat.getInstance();
	Integer year;
	String player; 
	Double regularSeasonERA,postseasonERA,regularSeasonWinLoss,postSeasonWinLoss;
	
	public static String COLUMNS() {
		return "Year, Player, Regular Season ERA, Regular Season Win/Loss, Post-season ERA, Post-season Win/Loss";
	}
	public void apply(Row value) {
		// TODO Auto-generated method stub
		//| playerID|YearID|RegularavgERA|PostavgERA|RegularWins|PostWins|RegularLosses|PostLosses
		this.year = value.getInt(1);
		this.regularSeasonWinLoss=value.getDouble(5);
		this.postSeasonWinLoss= value.getDouble(6);
		this.player = value.getString(0); 
		regularSeasonERA= value.getDouble(2);
		postseasonERA= value.getDouble(3);
	}
	public String toString() {
		StringBuilder sb= new StringBuilder();
		sb.append(FORMAT.format(year)).append(",")
		.append(player).append(",")
		.append(FORMAT.format(regularSeasonERA)).append(",")
		.append(FORMAT.format(regularSeasonWinLoss)).append(",")
		.append(FORMAT.format(postseasonERA)).append(",")
		.append(FORMAT.format(postSeasonWinLoss));
		return sb.toString();
	}
	
	//Mutator & Accessor methods
	public Integer getYear() {
		return year;
	}
	public void setYear(Integer year) {
		this.year = year;
	}
	public Double getRegularSeasonWinLoss() {
		return regularSeasonWinLoss;
	}
	public void setRegularSeasonWinLoss(Double regularSeasonWinLoss) {
		this.regularSeasonWinLoss = regularSeasonWinLoss;
	}
	public Double getPostSeasonWinLoss() {
		return postSeasonWinLoss;
	}
	public void setPostSeasonWinLoss(Double postSeasonWinLoss) {
		this.postSeasonWinLoss = postSeasonWinLoss;
	}
	public String getPlayer() {
		return player;
	}
	public void setPlayer(String player) {
		this.player = player;
	}
	public Double getRegularSeasonERA() {
		return regularSeasonERA;
	}
	public void setRegularSeasonERA(Double regularSeasonERA) {
		this.regularSeasonERA = regularSeasonERA;
	}
	public Double getPostseasonERA() {
		return postseasonERA;
	}
	public void setPostseasonERA(Double postseasonERA) {
		this.postseasonERA = postseasonERA;
	}

	

	
}
