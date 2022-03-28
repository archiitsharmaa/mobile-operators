package javadatabases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Query {
	
	
	// static variables accessed as global variables
	public static Logger logger = LogManager.getLogger(OperatorDatabase.class.getName());
	
	// default config routes
	public static final String DB_URL = "jdbc:mysql://localhost/";
	public static final String USER = "root";
	public static final String PASSWORD = "archit04";
	public static final String DATABASE = "MoblieOperator";
	public static final String OPERATOR_TABLE = "OPERATOR_DATA";
	public static final String MESSAGE_TABLE = "MESSAGE_DATA";

	
	
	
	
	public static void resourceIntializer() {
		// configures from the config files
		ReadProperties.getFile();
	}
	
	
	//prints query for the connection
	public static void printQuery(Connection connection, String query) {
		
		//query results
		try(Statement stmt = connection.createStatement()) {
		
		ResultSet resultset = stmt.executeQuery(query);
		
		//querying prints the query
		logger.info("executing query : "+ query);
		
		while(resultset.next()){
            //Display values
            logger.info("messages: " + resultset.getString("message"));
		}
		
		}
		
		catch(SQLException e) {
			logger.error("Error while executing query "+query);
		}
		
	}
	
	
	//creates connection with the server 
		public static Connection databaseConnector(String databaseURL, String userID, String password) throws Exception {
			
			//if the passed value from the configs is empty or missing it sets the default value
			if(databaseURL == null || databaseURL.isEmpty()) {
				logger.info("Default database url used");
				databaseURL = DB_URL;
			}
			
			if(userID == null || userID.isEmpty()) {
				logger.info("Default user ID used");
				userID = USER;
			}
			
			if(password == null || password.isEmpty()) {
				logger.info("Default password used");
				password = PASSWORD;
			}
				
			//creates connection object from the driver manager and throws exception if occured
			try{
				Connection connection = DriverManager.getConnection(databaseURL, userID, password);
				logger.info("Connected to server successfull");
				return connection;
				}
				catch(Exception e) {
					throw new Exception("Error occured while connecting to the server");
				}
		}

	
	
	
	
	public static void main(String[] args) {
		
		//intialies resources
		resourceIntializer();
		
		//fetching configs
		String databaseURL = ReadProperties.getResource("Db_URL");
		String userID = ReadProperties.getResource("User");
		String password = ReadProperties.getResource("Password"); 
		String databaseName = ReadProperties.getResource("Database");
		

		//queries
		String query1 = "Select MESSAGE FROM MESSAGE_DATA WHERE SENDER_NUMBER = 8872049908";
		String query2 = "Select MESSAGE FROM MESSAGE_DATA WHERE RECIEVER_NUMBER = 9872848978";
		String query3 = "Select MESSAGE FROM MESSAGE_DATA WHERE YEAR(SENT_TIME) BETWEEN 2021 AND 2022";
		String query4 = "Select MESSAGE FROM MESSAGE_DATA WHERE RECIEVER_NUMBER = 9872848978 AND FLOOR(((SENDER_NUMBER/100000) %10)) = 4";
		String query5 = "Select MESSAGE FROM MESSAGE_DATA WHERE RECIEVER_NUMBER = 8872349908 AND FLOOR(((SENDER_NUMBER/100000) %10)) = 4 AND FLOOR(SENDER_NUMBER/1000000) = 9872";
	 	String query6 = "Select MESSAGE FROM MESSAGE_DATA WHERE FLOOR(RECIEVER_NUMBER/100) = 98728489";
		String query7 = "Select MESSAGE FROM MESSAGE_DATA WHERE STATUS = 'failed' AND FLOOR(((SENDER_NUMBER/100000) %10)) = 4";
		
		
		//try resources to print the values 
		Connection connection;
		try {
			connection = databaseConnector(databaseURL+databaseName, userID, password);
			printQuery(connection, query1);
			printQuery(connection, query2);
			printQuery(connection, query3);
			printQuery(connection, query4);
			printQuery(connection, query5);
			printQuery(connection, query6);
			printQuery(connection, query7);
			
			
		} 
		
		catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage());
		}
		
		
	}
}