package javadatabases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
	 * Initialize table for storing operator details
	 * 
	 * @param connection    -> connection to database
	 * @param operatorTable -> table name
	 * @throws Exception
	 */
	public static void operatorRangeInfoTableIntializer(Connection connection, String operatorRangeTable)
			throws Exception {

		// if the passed value from the configs is empty or missing it sets the default value
		if (operatorRangeTable == null || operatorRangeTable.isEmpty()) {
			log.info("Default operator table name used");
			operatorRangeTable = OPERATOR_TABLE;
		}

		// creates MESSAGE table with mentioned field
		try (Statement stmt = connection.createStatement();) {
			String create_table = "CREATE TABLE " + operatorRangeTable + "(phone_range INT not NULL, "
					+ " operator VARCHAR(255), " + " PRIMARY KEY (phone_range ))";

			stmt.executeUpdate(create_table);
			log.info("Created " + operatorRangeTable + " table in given database");
		} catch (SQLException e) {
			// checks if the exception code matches the table exists code and then logs table exists error
			if (e.getErrorCode() == 1050) {
				log.info(operatorRangeTable + " already exists");
			}
			// throws any other exception if occurred
			else {
				throw new Exception("Error while creating operator table");
			}

		}
	}

	/**
	 * intilaize table for message transaction details
	 * 
	 * @param connection   -> connection to the database
	 * @param messageTable - table name string
	 * @throws Exception
	 */
	public static void messageInfoTableIntializer(Connection connection, String messageTable) throws Exception {

		// if the passed value from the configs is empty or missing it sets the default
		// value
		if (messageTable == null || messageTable.isEmpty()) {
			log.info("Default operator table name used");
			messageTable = MESSAGE_TABLE;
		}

		try (Statement stmt = connection.createStatement()) {
			String create_table = "CREATE TABLE " + messageTable + "(MESSAGEID INT not NULL, "
					+ " SENDER_NUMBER BIGINT not NULL, " + " RECIEVER_NUMBER BIGINT, " + " MESSAGE TEXT(255), "
					+ " SENT_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "
					+ " RECIVED_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " + " STATUS VARCHAR(255), "
					+ " PRIMARY KEY ( MESSAGEID ))";

			stmt.executeUpdate(create_table);
			log.info("Created message table in given database");
		} catch (SQLException e) {

			// checks if the exception code matches the table exists code and then logs
			// table exists error
			// changes
			if (e.getErrorCode() == 1050) {
				log.info("Message table already exists");
			}
			// throws any other exception if occurred
			else {
				throw new Exception("Error while creating message table");
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

			// inserts airtel numbers with the region code which starts from operatorRegions
			int airtelRange = 9872;

			// inserts jio numbers with the region code which starts from operatorRegions
			int jioRange = 8872;

			// inserts idea numbers with the region code which starts from operatorRegions
			int ideaRange = 9814;

			// inserts vi numbers with the region code which starts from operatorRegions
			int viRange = 6800;

			prepapredStatement.setInt(1, airtelRange);
			prepapredStatement.setString(2, "Airtel");
			prepapredStatement.addBatch();

			prepapredStatement.setInt(1, jioRange);
			prepapredStatement.setString(2, "jio");
			prepapredStatement.addBatch();

			prepapredStatement.setInt(1, ideaRange);
			prepapredStatement.setString(2, "Idea");
			prepapredStatement.addBatch();

			prepapredStatement.setInt(1, viRange);
			prepapredStatement.setString(2, "VI");
			prepapredStatement.addBatch();

			// executes all statements at once
			prepapredStatement.executeBatch();

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

		// inserting values in tables
		try (Statement statement = connection.createStatement()) {

			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (19, 9872448908, 9872848978, 'Hi ali hows you', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (1, 8872349908, 9872848988, 'we are going to have a problem soon bro', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (2, 6800449908, 8872349908, 'hehehehehhehe', NOW(), NOW(), 'failed')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (3, 6800349908, 8872349908, 'hehehehehhehe', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (4, 9814449908, 6800349908, 'its time to sleep', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (5, 6800449908, 9814009908, 'see you bro there', NOW(), NOW(), 'failed')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (6, 8872049908, 6800849908, 'same time same place', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (7, 8872449908, 8872949908, 'we are going to gym', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (8, 6800349998, 6800949908, 'dont call back ', NOW(), NOW(), 'failed')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (9, 9872449908, 8872349908, 'please call back', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (10, 6800449908, 9872848908, 'we ll probably not finish this by today', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (11, 9872848978, 8872349908, 'we ll do that', NOW(), NOW(), 'failed')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (12, 8872049908, 6800649908, 'thats it for you', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (13, 8872349908, 9814084897, 'they are late', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (14, 6800349976, 9814084897, 'rejected idea', NOW(), NOW(), 'failed')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (15, 9872448978, 8872349908, 'select them all', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (16, 8872349908, 6800349908, 'buy everything', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (17, 6800349934, 9872848921, 'got it', NOW(), NOW(), 'failed')");
			statement.addBatch("INSERT INTO " + messageTable
					+ " VALUES (18, 6800349999, 9872348908, 'thats it tired', NOW(), NOW(), 'Delivered')");

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
	 * Initialize table for storing region details
	 * 
	 * @param connection    -> connection to database
	 * @param operatorTable -> table name
	 * @throws Exception
	 */
	public static void operatorRegionTableIntializer(Connection connection, String operatorRegionTable)
			throws Exception {

		// if the passed value from the configs is empty or missing it sets the default value
		if (operatorRegionTable == null || operatorRegionTable.isEmpty()) {
			log.info("Default value table name used");
			operatorRegionTable = REGION_TABLE;
		}
	
		// creates MESSAGE table with mentioned field
		try (Statement stmt = connection.createStatement();) {
			String create_table = "CREATE TABLE " + operatorRegionTable + "(region_id INT not NULL, "
					+ " region VARCHAR(255), " + " PRIMARY KEY (region_id ))";

			stmt.executeUpdate(create_table);
			log.info("Created operator table in given database");
		} catch (SQLException e) {

			// checks if the exception code matches the table exists code and then logs table exists error
			if (e.getErrorCode() == 1050) {
				log.info(operatorRegionTable + " already exists");
			}
			// throws any other exception if occurred
			else {
				throw new Exception("Error while creating operator table");
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

			// intializes
			resourceIntializer();

			// intializing file path from the property
			// getresourcevalue some better name
			String databaseURL = ReadProperties.getValue("Db_URL");
			String userID = ReadProperties.getValue("User");
			String password = ReadProperties.getValue("Password");
			String databaseName = ReadProperties.getValue("Database");
			String operatorTable = ReadProperties.getValue("Operator_Table");
			String messageTable = ReadProperties.getValue("Message_Table");
			String regionTable = ReadProperties.getValue("region_Table");

			// crating connections to database
			connection = databaseConnector(databaseURL + databaseName, userID, password);

			// intializing tables
			operatorRangeInfoTableIntializer(connection, operatorTable);
			messageInfoTableIntializer(connection, messageTable);
			operatorRegionTableIntializer(connection, regionTable);

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
