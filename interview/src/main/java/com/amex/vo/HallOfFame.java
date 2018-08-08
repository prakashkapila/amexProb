package com.amex.vo;
import java.io.Serializable;

public class HallOfFame implements Serializable {
String player;
Double era;
Integer appearnaces;
int inductionYear;
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
}
