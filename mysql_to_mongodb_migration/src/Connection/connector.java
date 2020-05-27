package Connection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class connector 
{
    public static void main( String[] args ){
    	long start = System.currentTimeMillis();
    	List<String> table_list = new ArrayList<>();		
		List<String> field_name = new ArrayList<>();
		List<String> field_Type = new ArrayList<>();
		List<Document> doc = new ArrayList<>();
		
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter Database name: ");
		String db = sc.nextLine();
		
		try{
    		Class.forName("com.mysql.jdbc.Driver");
    		//change your username and password of mysql
    		Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/" + db,"root"/*usernmae*/,"root"/*password*/);
    		Statement stmt=con.createStatement();  
    		
    		ResultSet rs=stmt.executeQuery("show tables");
    		
    		while(rs.next()) {
    			table_list.add(rs.getString(1));
    		}
    		
    		MongoClient mongo = new MongoClient("localhost",27017);
    		MongoDatabase database = mongo.getDatabase("test");
    		
    		for(int i=0;i<table_list.size();i++) {
    			ResultSet schema=stmt.executeQuery("show columns from " + table_list.get(i));
    			while(schema.next()) {
    				field_name.add(schema.getString(1));
    				field_Type.add(schema.getString(2));
    			}
    			ResultSet table_data=stmt.executeQuery("select * from " + table_list.get(i));
    			database.createCollection(table_list.get(i));
    			
    			//iterate each table with each thread
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
    			MongoCollection<Document> collection = database.getCollection(table_list.get(i));
    			if(!doc.isEmpty()) {
    				collection.insertMany(doc);
    			}
    			field_name.clear();
    			field_Type.clear();
    			doc.clear();
    		}
    		con.close();
    	}
    	catch(Exception e){ 
    		System.out.println(e);
    	}
		long end = System.currentTimeMillis();
		System.out.println("Time Taken: " + ((end - start)*0.001) + "ms");
    }
}