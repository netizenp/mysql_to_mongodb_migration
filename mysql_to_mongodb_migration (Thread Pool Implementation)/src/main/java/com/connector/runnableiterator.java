package com.connector;

import java.sql.Connection;
import java.sql.ResultSet;

import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class runnableiterator implements Runnable {
	private Connection connection;
	private Statement stm1,stm2;
	private ResultSet schema,table_data;
	private Thread t;
	private String table_name;
	List<String> field_name = new ArrayList<String>();
	List<String> field_Type = new ArrayList<String>();
	List<Document> doc = new ArrayList<Document>();
	
	public runnableiterator(String table_name) {
		this.table_name = table_name;
	}
	@Override
	public void run() {
		try {
			MongoClient mongo = new MongoClient("localhost",27017);
    		MongoDatabase database = mongo.getDatabase("test");
			connections con = new connections();
			connection = con.getConnection();
    		stm1 = connection.createStatement();
    		schema = stm1.executeQuery("show columns from " + table_name);
			while(schema.next()) {
				field_name.add(schema.getString(1));
				field_Type.add(schema.getString(2));
			}
			stm2 = connection.createStatement();
    		table_data = stm2.executeQuery("select * from " + table_name);
			database.createCollection(table_name);
            
			while(table_data.next()) {
				Document document = new Document();
				for(int j=0;j<field_name.size();j++) {
					if(field_Type.get(j).equals("int")) {
						document.append(field_name.get(j), table_data.getInt(field_name.get(j)));
					}
					else {
						document.append(field_name.get(j), table_data.getString(field_name.get(j)));
					}
				}
				doc.add(document);
			}
			MongoCollection<Document> collection = database.getCollection(table_name);
			if(!doc.isEmpty()) {
				collection.insertMany(doc);
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
}