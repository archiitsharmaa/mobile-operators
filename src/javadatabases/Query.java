package javadatabases;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Query {
	
	
	// static variables accessed as global variables
	public static Logger logger = LogManager.getLogger(OperatorDatabase.class.getName());
		
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
	
	//function to execute queries
	public static void queryExceutor(Connection connection) {
		
		//queries
		String query1 = "Select MESSAGE FROM MESSAGE_DATA WHERE SENDER_NUMBER = 8872049908";
		String query2 = "Select MESSAGE FROM MESSAGE_DATA WHERE RECIEVER_NUMBER = 9872848978";
		String query3 = "Select MESSAGE FROM MESSAGE_DATA WHERE YEAR(SENT_TIME) BETWEEN 2021 AND 2022";
		String query4 = "Select MESSAGE FROM MESSAGE_DATA WHERE RECIEVER_NUMBER = 9872848978 AND FLOOR(((SENDER_NUMBER/100000) %10)) = 4";
		String query5 = "Select MESSAGE FROM MESSAGE_DATA WHERE RECIEVER_NUMBER = 8872349908 AND FLOOR(((SENDER_NUMBER/100000) %10)) = 4 AND FLOOR(SENDER_NUMBER/1000000) = 9872";
	 	String query6 = "Select MESSAGE FROM MESSAGE_DATA WHERE FLOOR(RECIEVER_NUMBER/100) = 98728489";
		String query7 = "Select MESSAGE FROM MESSAGE_DATA WHERE STATUS = 'failed' AND FLOOR(((SENDER_NUMBER/100000) %10)) = 4";
		
		
		//try resources to print the values 
		printQuery(connection, query1);
		logger.info("****");
		printQuery(connection, query2);
		logger.info("****");
		printQuery(connection, query3);
		logger.info("****");
		printQuery(connection, query4);
		logger.info("****");
		printQuery(connection, query5);
		logger.info("****");
		printQuery(connection, query6);
		logger.info("****");
		printQuery(connection, query7);
			
		
		
	}
}