package com.amex.vo.input;

import java.io.Serializable;

public class Appearances implements Serializable {
	/*
	 * Value Object to support `yearID` int(11) DEFAULT NULL, `teamID` varchar(255)
	 * DEFAULT NULL, `lgID` varchar(255) DEFAULT NULL, `playerID` varchar(255)
	 * DEFAULT NULL, `G_all` int(11) DEFAULT NULL, `GS` varchar(255) DEFAULT NULL,
	 * `G_batting` int(11) DEFAULT NULL, `G_defense` int(11) DEFAULT NULL, `G_p`
	 * int(11) DEFAULT NULL, `G_c` int(11) DEFAULT NULL, `G_1b` int(11) DEFAULT
	 * NULL, `G_2b` int(11) DEFAULT NULL, `G_3b` int(11) DEFAULT NULL, `G_ss`
	 * int(11) DEFAULT NULL, `G_lf` int(11) DEFAULT NULL, `G_cf` int(11) DEFAULT
	 * NULL, `G_rf` int(11) DEFAULT NULL, `G_of` int(11) DEFAULT NULL, `G_dh`
	 * varchar(255) DEFAULT NULL, `G_ph` varchar(255) DEFAULT NULL, `G_pr`
	 * varchar(255) DEFAULT NULL
	 * 
	 */
	Integer yearId, gall,gbatting,gdefense, gp, gc, g1b, g2b, g3b, gss, glf, grf, gof,gcf;
	String teamID, lgId, playerId, gs, gdh, gph, gpr;

	public Integer getYearId() {
		return yearId;
	}

	public void setYearId(Integer yearId) {
		this.yearId = yearId;
	}

	public Integer getGall() {
		return gall;
	}

	public void setGall(Integer gall) {
		this.gall = gall;
	}

	public Integer getGp() {
		return gp;
	}

	public void setGp(Integer gp) {
		this.gp = gp;
	}

	public Integer getGc() {
		return gc;
	}

	public void setGc(Integer gc) {
		this.gc = gc;
	}

	public Integer getG1b() {
		return g1b;
	}

	public void setG1b(Integer g1b) {
		this.g1b = g1b;
	}

	public Integer getG2b() {
		return g2b;
	}

	public void setG2b(Integer g2b) {
		this.g2b = g2b;
	}

	public Integer getG3b() {
		return g3b;
	}

	public void setG3b(Integer g3b) {
		this.g3b = g3b;
	}

	public Integer getGss() {
		return gss;
	}

	public void setGss(Integer gss) {
		this.gss = gss;
	}

	public Integer getGlf() {
		return glf;
	}

	public void setGlf(Integer glf) {
		this.glf = glf;
	}

	public Integer getGrf() {
		return grf;
	}

	public void setGrf(Integer grf) {
		this.grf = grf;
	}

	public Integer getGof() {
		return gof;
	}

	public void setGof(Integer gof) {
		this.gof = gof;
	}

	public String getTeamID() {
		return teamID;
	}

	public void setTeamID(String teamID) {
		this.teamID = teamID;
	}

	public String getLgId() {
		return lgId;
	}

	public void setLgId(String lgId) {
		this.lgId = lgId;
	}

	public String getPlayerId() {
		return playerId;
	}

	public void setPlayerId(String playerId) {
		this.playerId = playerId;
	}

	public String getGs() {
		return gs;
	}

	public void setGs(String gs) {
		this.gs = gs;
	}

	public String getGdh() {
		return gdh;
	}

	public void setGdh(String gdh) {
		this.gdh = gdh;
	}

	public String getGph() {
		return gph;
	}

	public void setGph(String gph) {
		this.gph = gph;
	}

	public String getGpr() {
		return gpr;
	}

	public void setGpr(String gpr) {
		this.gpr = gpr;
	}

	public void apply(Object object, int i) {
		//  | teamID | lgID | playerID | G_all
		try {
			
			switch (i) {
			case 1: {
				Integer yr = object == null ? 0:Integer.valueOf(String.valueOf(object));
				this.yearId = yr;
 				break;
			}
			case 2: {
				this.teamID =object == null ? "":String.valueOf(object);; 
				break;
			}
			case 3: {
				 this.lgId =object == null ? "":String.valueOf(object); 
				break;
			}
			
			case 4: { 
				this.playerId =object == null ? "":String.valueOf(object);
				break;
			}
			case 5: { 
				this.gall = object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
	 		case 6: { 
				this.gs =object == null ? "":String.valueOf(object);
				break;
			}
	 		case 7: { 
				this.gbatting =object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
			case 8: { 
				this.gdefense =object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
			case 9: { 
				this.gp =object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
			case 10: { 
				this.gc =object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
			case 11: { 
				this.g1b =object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
			case 12: { 
				this.g2b =object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
		 	case 13: { 
				this.g3b =object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
			case 14: { 
				this.gss =object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
			case 15: { 
				this.glf =object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
		 	case 16: { 
				this.gcf =object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
			case 17: { 
				this.grf =object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
			case 18: { 
				this.gof = object == null ? 0:Integer.valueOf(String.valueOf(object));;
				break;
			}
			case 19: { 
				this.gdh =object == null ? "":String.valueOf(object);
				break;
			}
			case 20: { 
				this.gph = object == null ? "":String.valueOf(object);
				break;
			}
			}

		} catch (Exception esp) {
			System.out.println("Broke conversion on " + object + "i as" + i);
		}
	}
}
