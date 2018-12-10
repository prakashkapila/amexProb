package com.durgaveg.learningSpark.ml.predictions;

import java.io.Serializable;

import org.apache.spark.mllib.recommendation.Rating;

public class UserArtists implements Serializable {
 	private static final long serialVersionUID = 768666598886643014L;
	String userid,timestamp,musicbrainzArtistId,artistName,musicbrainzTrackId,trackName;
	  
	public UserArtists(String[] row) {
		 
 		if(row.length < 1)
		{
 			System.out.println("The Row does not have proper data"+row);
 			return ;
		}
 		for( int i=0;i<row.length;i++)
 			switch(i)
 			{
 			case 0:{
 				userid=row[i];
 				break;
 			}
 			case 1:{
 				timestamp = row[i];
 			}
 			case 2:{
 				musicbrainzArtistId = row[i];
 			}
 			case 3:{
 				artistName = row[i];
 			}
 			case 4:{
 				musicbrainzTrackId = row[i];
 			}
 			case 5:{
 				musicbrainzTrackId = row[i];
 			}
 			}
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getMusicbrainzArtistId() {
		return musicbrainzArtistId;
	}

	public void setMusicbrainzArtistId(String musicbrainzArtistId) {
		this.musicbrainzArtistId = musicbrainzArtistId;
	}

	public String getArtistName() {
		return artistName;
	}

	public void setArtistName(String artistName) {
		this.artistName = artistName;
	}

	public String getMusicbrainzTrackId() {
		return musicbrainzTrackId;
	}

	public void setMusicbrainzTrackId(String musicbrainzTrackId) {
		this.musicbrainzTrackId = musicbrainzTrackId;
	}

	public String getTrackName() {
		return trackName;
	}

	public void setTrackName(String trackName) {
		this.trackName = trackName;
	}
}