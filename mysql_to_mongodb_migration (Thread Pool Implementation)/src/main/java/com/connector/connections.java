package com.connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class connections {
	private Connection con;
	public Connection getConnection() {
		try{
    		Class.forName("com.mysql.jdbc.Driver");
    		con=DriverManager.getConnection("jdbc:mysql://localhost:3306/octoscope","root","123456789A");
    	}
		catch(Exception e) {
			System.out.println(e);
		}
		return con;
	}
}