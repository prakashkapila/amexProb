package com.amex.vo;
import java.io.Serializable;

public class Pitching implements Serializable{
	 
	private static final long serialVersionUID = 3615110930547267695L;
	
	Integer year,regularSeasonWinLoss,postSeasonWinLoss;
	String player; 
	Double regularSeasonERA,postseasonERA;
	
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
