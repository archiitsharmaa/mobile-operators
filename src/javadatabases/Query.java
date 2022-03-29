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
		
	/**
	 * prints query for the connection
	 * @param connection - connection to the object
	 * @param query -> query string
	 */
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
	
	
	/**
	 * function to execute queries
	 * @param connection -> connects to the object
	 */
	public static void queryExceutor(Connection connection) {
		
		//queries
		//number input from queries
		String query1 = "Select MESSAGE FROM MESSAGE_DATA WHERE SENDER_NUMBER = 8872049908";
		String query2 = "Select MESSAGE FROM MESSAGE_DATA WHERE RECIEVER_NUMBER = 9872848978";
		String query3 = "Select MESSAGE FROM MESSAGE_DATA WHERE YEAR(SENT_TIME) BETWEEN 2021 AND 2022";
		//the number '4' on the 5th digit of a phone number represents punjab region for all operators
		
		String query44 = "Select MESSAGE FROM MESSAGE_DATA WHERE RECIEVER_NUMBER = 9872848978 AND FLOOR(((SENDER_NUMBER/100000) %10)) = 4";
		
		

		String query4 = "Select A.MESSAGE FROM MESSAGE_DATA A INNER JOIN REGIONINFO b on FLOOR(((a.SENDER_NUMBER/100000) %10)) = b.region_id WHERE a.RECIEVER_NUMBER = 9872848978 ";
		String query55 = "Select MESSAGE FROM MESSAGE_DATA WHERE RECIEVER_NUMBER = 8872349908 AND FLOOR(((SENDER_NUMBER/100000) %10)) = 4 "
				+ "AND FLOOR(SENDER_NUMBER/1000000) = 9872";
		
		String query5 = "SELECT A.MESSAGE FROM MESSAGE_DATA A INNER JOIN ON REGIONINFO B ON FLOOR(((A.SENDER_NUMBER/100000) %10)) = B.REGION_ID inner join operator_range_data c on FLOOR(a.SENDER_NUMBER/1000000) = c.phone_range;";
		
		
	 	String query6 = "Select MESSAGE FROM MESSAGE_DATA WHERE FLOOR(RECIEVER_NUMBER/100) = 98728489";
		String query7 = "Select MESSAGE FROM MESSAGE_DATA WHERE STATUS = 'failed' AND FLOOR(((SENDER_NUMBER/100000) %10)) = (select region_id from regioninfo where region = 'Punjab')";
		
		
		//String query4 = "Select MESSAGE FROM MESSAGE_DATA WHERE RECIEVER_NUMBER = 9872848978 AND FLOOR(((SENDER_NUMBER/100000) %10)) = (select region_id from regioninfo where region = 'Punjab')";
		
		
		//try resources to print the values 
		//printQuery(connection, query1);
		//logger.info("****");
		//printQuery(connection, query2);
		//logger.info("****");
		//printQuery(connection, query3);
		//logger.info("****");
		//printQuery(connection, query4);
		logger.info("****");
		printQuery(connection, query5);
		//logger.info("****");
		//printQuery(connection, query6);
		//logger.info("****");
		//printQuery(connection, query7);
			
		
		
	}
}