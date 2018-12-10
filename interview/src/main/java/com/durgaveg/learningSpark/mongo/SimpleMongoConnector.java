package com.durgaveg.learningSpark.mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;

public class SimpleMongoConnector implements Serializable{
	static final MongoClientURI CONNECTION;
	static final MongoClient CLIENT;
 	static final MongoDatabase DATABASE ;
	static {
		CONNECTION = new MongoClientURI("mongodb://localhost:27017");
		 CLIENT= new MongoClient(CONNECTION);
		 DATABASE=CLIENT.getDatabase("test");
	}
	MongoCollection coll = null;
	public SimpleMongoConnector (String table)
	{
		coll = DATABASE.getCollection(table);
	}
	public void connectTable(String table)
	{
		coll = DATABASE.getCollection(table);
	}
	
	public void insertMany(List<Document> doc,String collection)
	{
		coll = coll != null ? coll
				:DATABASE.getCollection(collection);
		 coll.insertMany(doc);
	}
	public void insertOne(Document doc)
	{
		 coll.insertOne(doc);
	}
	public void close() {
		 CLIENT.close();
	}
	public void testConnection() {
		 MongoClientURI connectionString = new MongoClientURI("mongodb://localhost:27017");
		 MongoClient mongoClient = new MongoClient(connectionString);
		 MongoDatabase database = mongoClient.getDatabase("test");
		 Document doc = new Document("name", "MongoDB")
	                .append("type", "database")
	                .append("count", 1)
	                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
	                .append("info", new Document("x", 203).append("y", 102));
		 MongoCollection coll = database.getCollection("SomeCollection");
		 coll.insertOne(doc);
		 mongoClient.close();
	}
	 
	public static void main(String arg[])
	{
		SimpleMongoConnector conn = new SimpleMongoConnector("SomeCollection");
		conn.testConnection();
	}
}
