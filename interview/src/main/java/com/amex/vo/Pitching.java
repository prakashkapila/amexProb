package com.amex.vo;
import java.io.Serializable;
import java.text.NumberFormat;

public class Pitching implements Serializable{
	 
	private static final long serialVersionUID = 3615110930547267695L;
	private static final NumberFormat FORMAT = NumberFormat.getInstance();
	Integer year,regularSeasonWinLoss,postSeasonWinLoss;
	String player; 
	Double regularSeasonERA,postseasonERA;
	
	public static String getCols() {
		return "Year, Player, Regular Season ERA, Regular Season Win/Loss, Post-season ERA, Post-season Win/Loss";
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
	public Integer getRegularSeasonWinLoss() {
		return regularSeasonWinLoss;
	}
	public void setRegularSeasonWinLoss(Integer regularSeasonWinLoss) {
		this.regularSeasonWinLoss = regularSeasonWinLoss;
	}
	public Integer getPostSeasonWinLoss() {
		return postSeasonWinLoss;
	}
	public void setPostSeasonWinLoss(Integer postSeasonWinLoss) {
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
