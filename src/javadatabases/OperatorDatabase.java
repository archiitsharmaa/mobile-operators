package javadatabases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.ArrayUtils;

/**
 * create a mobile operator database in the system populate them with ranges
 * from different operators and regions maintain a system of messages sent from
 * one number to the other
 * 
 * @author archit.sharma
 *
 */

public class OperatorDatabase {

	// static variables accessed as global variables
	public static Logger log = LogManager.getLogger(OperatorDatabase.class.getName());

	// default config routes
	public static final String OPERATOR_TABLE = "OPERATOR_Range_DATA";
	public static final String MESSAGE_TABLE = "MESSAGE_DATA";
	public static final String REGION_TABLE = "regionTable";

	/**
	 * Initializes the config file
	 */
	public static void resourceIntializer() {
		// configures from the config files
		ReadProperties.getFile();
	}

	/**
	 * creates connection with the server
	 * 
	 * @param databaseURL -> database url
	 * @param userID      -> user id
	 * @param password    -> password
	 * @return -> returns connection object
	 * @throws Exception
	 */
	public static Connection databaseConnector(String databaseURL, String userID, String password) throws Exception {

		// creates connection object from the driver manager and throws exception if occured
		try {
			Connection connection = DriverManager.getConnection(databaseURL, userID, password);
			log.info("Connected to server successfull");
			return connection;
		} catch (SQLException e) {

			if (e.getErrorCode() == 1045) {
				throw new Exception("error in userID or password, cannot connect");
			} else {
				System.out.println(e);
				throw new Exception("Error occured while connecting to the server");
			}
		}
	}
	
	/**
	 * 
	 * @param connection
	 * @param databaseTable
	 * @param tableId
	 * @param tableAttribute
	 * @throws Exception
	 */
	public static void tableIntializer(Connection connection, String databaseTable, int tableId, String tableAttribute) throws Exception {
		
		// if the passed value from the configs is empty or missing it sets the default value
		if (databaseTable == null || databaseTable.isEmpty())  {
			
			if(tableId == 1) {
			log.info("Default operator range table name used");
			databaseTable = OPERATOR_TABLE;
			}	
			else if(tableId == 2) {
				log.info("Default region table name used");
				databaseTable = REGION_TABLE;
			}
			else if(tableId == 3) {
				log.info("Default message table name used");
				databaseTable = MESSAGE_TABLE;
			}
		}
		
		try (Statement stmt = connection.createStatement();) {
			
			String create_table = "CREATE TABLE " + databaseTable + tableAttribute;
			stmt.executeUpdate(create_table);
			
			log.info("Created " + databaseTable + " table in given database");
		} catch (SQLException e) {
			// checks if the exception code matches the table exists code and then logs table exists error
			if (e.getErrorCode() == 1050) {
				log.info(databaseTable + " already exists");
			}
			// throws any other exception if occurred
			else {
				throw new Exception("Error while creating " + databaseTable);
			}

		}
		
	}

	/**
	 * inserts / populates the message table
	 * 
	 * @param connection    -> connection to the database
	 * @param operatorTable -> message table name
	 * @throws Exception
	 */
	public static void insertOperatorRangeValue(Connection connection, String operatorRangeTable) throws Exception {

		// if the passed value from the configs is empty or missing it sets the default value
		if (operatorRangeTable == null || operatorRangeTable.isEmpty()) {
			log.info("Default operator table used");
			operatorRangeTable = OPERATOR_TABLE;
		}

		// inserts value in the table and exits if insert errors exists
		try {

			// prepared statement to insert in table
			PreparedStatement prepapredStatement = connection
					.prepareStatement("INSERT INTO " + operatorRangeTable + " VALUES (?, ?)");
			
			//operator names
			String[] operators = new String[] {"airtelRange", "jioRange", "ideaRange", "viRange"};
			
			//phone range
			int[] range = new int[] {9872, 8872, 9814, 6800};
			
			//map the operator to ranges
			Map<String, Integer> operatorRangeMap = IntStream.range(0, operators.length).boxed()
				    .collect(Collectors.toMap(i -> operators[i], i -> range[i]));
			
			for(Map.Entry<String, Integer> rangeMap : operatorRangeMap.entrySet()) {
				
				prepapredStatement.setInt(1,rangeMap.getValue());
				prepapredStatement.setString(2, rangeMap.getKey());
				prepapredStatement.addBatch();
			}

			log.info("Inserted operator values");

		} catch (SQLException e) {

			// checks if the exception code matches the duplicate inserts in primary key
			if (e.getErrorCode() == 1062) {
				log.info("Duplicate values inserted in operator table");
			}
			// throws any other exception if occurred
			else {
				throw new Exception("Error while inserting in operator table");
			}

		}

	}

	/**
	 * inserts / populates the message table
	 * 
	 * @param connection   -> connection to the database
	 * @param messageTable -> table name
	 * @throws Exception
	 */
	public static void insertMessageDetails(Connection connection, String messageTable) throws Exception {

		// if the passed value from the configs is empty or missing it sets the default
		// value
		if (messageTable == null || messageTable.isEmpty()) {
			log.info("Default message table used");
			messageTable = MESSAGE_TABLE;
		}

		
		ArrayList<String> messagesInsert = new ArrayList<String>();

		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (19, 9872448908, 9872848978, 'Hi ali hows you', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (1, 8872349908, 9872848988, 'we are going to have a problem soon bro', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (2, 6800449908, 8872349908, 'hehehehehhehe', NOW(), NOW(), 'failed')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (3, 6800349908, 8872349908, 'hehehehehhehe', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (4, 9814449908, 6800349908, 'its time to sleep', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (5, 6800449908, 9814009908, 'see you bro there', NOW(), NOW(), 'failed')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (6, 8872049908, 6800849908, 'same time same place', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (7, 8872449908, 8872949908, 'we are going to gym', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (8, 6800349998, 6800949908, 'dont call back ', NOW(), NOW(), 'failed')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (9, 9872449908, 8872349908, 'please call back', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (10, 6800449908, 9872848908, 'we ll probably not finish this by today', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (11, 9872848978, 8872349908, 'we ll do that', NOW(), NOW(), 'failed')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (12, 8872049908, 6800649908, 'thats it for you', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (13, 8872349908, 9814084897, 'they are late', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (14, 6800349976, 9814084897, 'rejected idea', NOW(), NOW(), 'failed')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (15, 9872448978, 8872349908, 'select them all', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (16, 8872349908, 6800349908, 'buy everything', NOW(), NOW(), 'Delivered')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (17, 6800349934, 9872848921, 'got it', NOW(), NOW(), 'failed')");
		messagesInsert.add("INSERT INTO " + messageTable
				+ " VALUES (18, 6800349999, 9872348908, 'thats it tired', NOW(), NOW(), 'Delivered')");
		
		

		// inserting values in tables
		try (Statement statement = connection.createStatement()) {
			
			//add into the insert
			for(int i=0;i<messagesInsert.size();i++) {
				statement.addBatch(messagesInsert.get(i));
			}
			
			statement.executeBatch();

			log.info("Inserted message details values");

		}

		catch (SQLException e) {
			// checks if the exception code matches the duplicate inserts in primary key
			if (e.getErrorCode() == 1062) {
				log.info("Duplicate values inserted in message table");
			}
			// throws any other exception if occurred
			else {
				throw new Exception("Error while inserting in message table");
			}

		}

	}

	
	/**
	 * fills value for the region value
	 * @param connection
	 * @param operatorRegionTable
	 * @throws Exception
	 */
	public static void insertOperatorRegionValue(Connection connection, String operatorRegionTable) throws Exception {

		// if the passed value from the configs is empty or missing it sets the default value
		if (operatorRegionTable == null || operatorRegionTable.isEmpty()) {
			log.info("Default operator table used");
			operatorRegionTable = REGION_TABLE;
		}
		

		// inserts value in the table and exits if insert errors exists
		try {

			// prepared statement to insert in table
			PreparedStatement prepapredStatement = connection
					.prepareStatement("INSERT INTO " + operatorRegionTable + " VALUES (?, ?)");

			// string array of regions
			String[] operatorRegions = new String[] { "Himachal Pradesh", "Jammu and Kashmir", "Ladakh", "Uttrakhand",
					"Punjab", "Haryana", "Delhi", "Uttar Pradesh", "Rajasthan", "Bihar" };

			// inserts airtel numbers with the region code which starts from operatorRegions
			// string array's index
			for (int i = 0; i < operatorRegions.length; i++) {

				prepapredStatement.setInt(1, i);
				prepapredStatement.setString(2, operatorRegions[i]);
				prepapredStatement.addBatch();

			}

			// executes all statements at once
			prepapredStatement.executeBatch();

			log.info("Inserted region values");

		} catch (SQLException e) {

			// checks if the exception code matches the duplicate inserts in primary key
			if (e.getErrorCode() == 1062) {
				log.info("Duplicate values inserted in region table");
			}
			// throws any other exception if occurred
			else {
				throw new Exception("Error while inserting in operator table");
			}

		}

	}

	// main function

	public static void main(String[] args) throws SQLException {
		
		Connection connection = null;

		try {

			// Initializes
			resourceIntializer();

			// Initializing file path from the property
			String databaseURL = ReadProperties.getValue("Db_URL");
			String userID = ReadProperties.getValue("User");
			String password = ReadProperties.getValue("Password");
			String databaseName = ReadProperties.getValue("Database");
			String operatorTable = ReadProperties.getValue("Operator_Table");
			String messageTable = ReadProperties.getValue("Message_Table");
			String regionTable = ReadProperties.getValue("region_Table");

			// crating connections to database
			connection = databaseConnector(databaseURL + databaseName, userID, password);
			
			String operator_attribute = "(phone_range INT not NULL, "
					+ " operator VARCHAR(255), " + 
					" PRIMARY KEY (phone_range ))";
			
			String region_attribute = "(region_id INT not NULL, "
					+ " region VARCHAR(255), " + 
					" PRIMARY KEY (region_id ))";
			
			String message_attribute = "(MESSAGEID INT not NULL, "
					+ " SENDER_NUMBER BIGINT not NULL, " + " RECIEVER_NUMBER BIGINT, " + " MESSAGE TEXT(255), "
					+ " SENT_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
					+ " RECIVED_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " + " STATUS VARCHAR(255), "
					+ " PRIMARY KEY ( MESSAGEID ))";
			
			
			//intiaze tables
			tableIntializer(connection, operatorTable, 1, operator_attribute);
			tableIntializer(connection, regionTable, 2, region_attribute);
			tableIntializer(connection, messageTable, 3, message_attribute);
			

			// inserting values in tables
			insertOperatorRangeValue(connection, operatorTable);
			insertMessageDetails(connection, messageTable);
			insertOperatorRegionValue(connection, regionTable);

			// executing query
			Query.queryExceutor(connection);



		} catch (Exception e) {
			// connection.close();
			log.error(e.getMessage());
		}
		
		finally {
			if(connection != null) {
				// closing the connection
				connection.close();
			}
		}
	}

}
