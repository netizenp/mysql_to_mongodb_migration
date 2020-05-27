package com.connector;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class App 
{
	private static Connection connection;
	private static Statement stm;
	private static ResultSet result;
    public static void main( String[] args ){
    	long start = System.currentTimeMillis();
    	List<String> table_list = new ArrayList<>();
    	connections con = new connections();
		try{
			connection = con.getConnection();
    		stm = connection.createStatement();
    		result = stm.executeQuery("show tables");
    		while(result.next()) {
    			table_list.add(result.getString(1));
    		}
    		
    		ExecutorService pool = Executors.newFixedThreadPool(4);
    		for(int i=0;i<table_list.size();i++) {
    			pool.execute(new runnableiterator(table_list.get(i)));
    		}
    	}
    	catch(Exception e){ 
    		System.out.println(e);
    	}
		
		long end = System.currentTimeMillis();
		System.out.println("Time Taken: " + ((end - start)*0.001) + "ms");
    }
}