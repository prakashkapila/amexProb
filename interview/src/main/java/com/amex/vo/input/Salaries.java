package com.amex.vo.input;

import java.io.Serializable;

public class Salaries implements Serializable {
	/**
		 * 
		 */
	private static final long serialVersionUID = -8342551942059278107L;
	/*
	 * `yearID` int(11) DEFAULT NULL, `teamID` varchar(255) DEFAULT NULL, `lgID`
	 * varchar(255) DEFAULT NULL, `playerID` varchar(255) DEFAULT NULL, `salary`
	 * int(11) DEFAULT NULL
	 */
	Integer yearId;
	String teamId, playerId, lgId;
	Double salary;

	public void apply(Object object, int i) {
		// TODO Auto-generated method stub
		try {
			switch (i) {
			case 1: {
				Integer yr = Integer.valueOf(String.valueOf(object));
				this.yearId = yr;
				break;
			}
			case 2: {

				this.teamId = String.valueOf(object);
				break;
			}
			case 3: {
				this.lgId = String.valueOf(object);
				break;
			}
			case 4: {
				this.playerId = String.valueOf(object);
				break;
			}
			case 5: {
				Double sal = Double.valueOf(String.valueOf(object));
				this.salary = sal;
				break;
			}
			}
		} catch (Exception esp) {
			System.out.println("Broke conversion on " + object + "i as" + i);
		}
	}

	public Integer getYearId() {
		return yearId;
	}

	public void setYearId(Integer yearId) {
		this.yearId = yearId;
	}

	public String getTeamId() {
		return teamId;
	}

	public void setTeamId(String teamId) {
		this.teamId = teamId;
	}

	public String getPlayerId() {
		return playerId;
	}

	public void setPlayerId(String playerId) {
		this.playerId = playerId;
	}

	public String getLgId() {
		return lgId;
	}

	public void setLgId(String lgId) {
		this.lgId = lgId;
	}

	public Double getSalary() {
		return salary;
	}

	public void setSalary(Double salary) {
		this.salary = salary;
	}

}
