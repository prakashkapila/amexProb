package com.durgaveg.learningSpark.ml.predictions;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
 

public class ALSRecommender{
	JavaSparkContext  session;
	String alsFile="C:\\data\\db\\music\\lastfm-dataset-1K\\userid-timestamp-artid-artname-traid-traname.tsv";
	String delimiter = "\t";
	String metaData ="userid,timestamp,musicbrainz-artist-id,artist-name,musicbrainz-track-id,track-name";
	
	public ALSRecommender(JavaSparkContext session)
	{
		this.session = session;
	}
	
	public void performALS(){
		JavaRDD<String> lines = session.textFile(alsFile);
		JavaRDD<UserArtists> userArtists = lines.flatMap(new FlatMapFunction<String,UserArtists>(){
		 	private static final long serialVersionUID = 4215582288195361808L;
			String[] row= null;
			@Override
			public Iterator<UserArtists> call(String v1) throws Exception {
				row = v1.split("\t");
				UserArtists rating = new UserArtists(row);
				ArrayList<UserArtists> rats = new ArrayList<UserArtists>();
				rats.add(rating);
				return rats.iterator();
			}});
		
	}
	
}
