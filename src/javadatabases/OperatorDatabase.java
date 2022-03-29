package javadatabases;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * create a mobile operator database in the system
 * populate them with ranges from different operators and regions
 * maintain a system of messages sent from one number to the other
 * 
 * @author archit.sharma
 *
 */


public class OperatorDatabase {
	
	
	// static variables accessed as global variables
	public static Logger log = LogManager.getLogger(OperatorDatabase.class.getName());
	
	// default config routes
	public static final String DATABASE = "MobileOperator";
	public static final String OPERATOR_TABLE = "OPERATOR_DATA";
	public static final String MESSAGE_TABLE = "MESSAGE_DATA";

	
	/**
	 * Initializes the config file
	 */
	public static void resourceIntializer() {
		// configures from the config files
		ReadProperties.getFile();
	}
	
	
	/**
	 * creates connection with the server 
	 * @param databaseURL -> database url
	 * @param userID -> user id
	 * @param password -> password
	 * @return -> returns connection object
	 * @throws Exception
	 */
	public static Connection databaseConnector(String databaseURL, String userID, String password) throws Exception {
		
		//string utils
		//url should not be default
		//url + string if database connects the statement
			
		//creates connection object from the driver manager and throws exception if occured
		try{
			Connection connection = DriverManager.getConnection(databaseURL, userID, password);
			log.info("Connected to server successfull");
			return connection;
			}
			catch(SQLException e) {
				
				if(e.getErrorCode() == 1045){
					throw new Exception("error in userID or password, cannot connect");
				}
				else {
				throw new Exception("Error occured while connecting to the server");
				}
			}
	}
	
		
	/**
	 * create database in the server
	 * @param connection -> connection to the database
	 * @param databaseName -> database name
	 * @throws Exception
	 */
	public static void databaseIntializer(Connection connection, String databaseName) throws Exception {
		
		//if the passed value from the configs is empty or missing it sets the default value
		if(databaseName == null || databaseName.isEmpty()) {
			log.info("Default database name used");
			databaseName = DATABASE;
		}
		
		//creates database with the passed database name
		try(Statement stmt = connection.createStatement()) {		      
	         String createDatabase = "CREATE DATABASE " + databaseName;
	         stmt.executeUpdate(createDatabase);
	         log.info("Database created successfully");   	  
		} 
		catch (SQLException e) {
			
			//checks if the exception code matches the database exists code and logs database exists error
			if(e.getErrorCode() == 1007) {
				log.info("Database already exists");
			}
			//throws any other exception if occurred
			else {
				throw new Exception("Error while creating database");
			}
		} 
	}
	
	
	
	/**
	 * selects database
	 * @param connection -> connetion for the server
	 * @param database -> database name
	 * @throws Exception
	 */
	public static void databaseSelector(Connection connection, String database) throws Exception {
		
		//if the passed value from the configs is empty or missing it sets the default value
		if(database == null || database.isEmpty()) {
			log.info("Default database name used");
			database = DATABASE;
		}
		
		//selects database for the passed name and if error occurred throws it
		try(Statement stmt = connection.createStatement()) {		  
				
         String selectDatabase = "USE " + database;
         stmt.executeUpdate(selectDatabase);
         log.info("Database selected successfully");   	  
         
		} 
		
		catch (SQLException e) {
		 	  throw new Exception("Error while selecting the database");
		}
		
	}

	
	/**
	 * Initialize table for storing operator details
	 * @param connection -> connection to database
	 * @param operatorTable -> table name
	 * @throws Exception
	 */
	public static void operatorRangeInfoTableIntializer(Connection connection, String operatorRangeTable) throws Exception{
		
		//if the passed value from the configs is empty or missing it sets the default value
//		if(operatorTable == null || operatorTable.isEmpty()) {
//			log.info("Default operator table name used");
//			operatorTable = OPERATOR_TABLE;
//		}		
		
		//creates MESSAGE table with mentioned field
		try(Statement stmt = connection.createStatement();) {		      
		          String create_table = "CREATE TABLE " + operatorRangeTable +
		                   "(phone_range INT not NULL, " +
		                   " operator VARCHAR(255), " + 
		       		       " PRIMARY KEY (phone_range ))"; 

		         stmt.executeUpdate(create_table);
		         log.info("Created operator table in given database");   	  
		      } 
		catch (SQLException e) {
			System.out.println(e.getMessage());
			   	  
    	//checks if the exception code matches the table exists code and then logs table exists error
			if(e.getErrorCode() == 1050) {
				log.info(operatorRangeTable+" already exists");
			}
			//throws any other exception if occurred
			else {
				throw new Exception("Error while creating operator table");
			}
	         
		  }
	}
	
	
	
	/**
	 * intilaize table for message transaction details
	 * @param connection -> connection to the database
	 * @param messageTable - table name string
	 * @throws Exception
	 */
	public static void messageInfoTableIntializer(Connection connection, String messageTable) throws Exception {
		
		//if the passed value from the configs is empty or missing it sets the default value
		if(messageTable == null || messageTable.isEmpty()) {
			log.info("Default operator table name used");
			messageTable = MESSAGE_TABLE;
		}
		
		try(Statement stmt = connection.createStatement()) {		      
		          String create_table = "CREATE TABLE " + messageTable +
		        		   "(MESSAGEID INT not NULL, " +
		                   " SENDER_NUMBER BIGINT not NULL, " +
		                   " RECIEVER_NUMBER BIGINT, " + 
		                   " MESSAGE TEXT(255), " +
		                   " SENT_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " + 
		                   " RECIVED_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " + 
		                   " STATUS VARCHAR(255), " +
		                   " PRIMARY KEY ( MESSAGEID ))"; 

		         stmt.executeUpdate(create_table);
		         log.info("Created message table in given database");   	  
		      } 
		catch (SQLException e) {
			
			//checks if the exception code matches the table exists code and then logs table exists error
			//changes
			if(e.getErrorCode() == 1050) {
				log.info("Message table already exists");
			}
			//throws any other exception if occurred
			else {
				throw new Exception("Error while creating message table");
			}
	         
		  }		 		
	}
	
	
	/**
	 * inserts / populates the message table
	 * @param connection -> connection to the database
	 * @param operatorTable -> message table name
	 * @throws Exception
	 */
	public static void insertOperatorRangeValue(Connection connection, String operatorRangeTable) throws Exception {
		
		//if the passed value from the configs is empty or missing it sets the default value
		if(operatorRangeTable == null || operatorRangeTable.isEmpty()) {
			log.info("Default operator table used");
			operatorRangeTable = OPERATOR_TABLE;
		}	
		
		//inserts value in the table and exits if insert errors exists
		try {
		
		//prepared statement to insert in table	
		PreparedStatement prepapredStatement = connection.prepareStatement("INSERT INTO " + operatorRangeTable + " VALUES (?, ?)");
		
		//inserts airtel numbers with the region code which starts from operatorRegions string array's index
		int airtelRange = 9872;
		
		//inserts jio numbers with the region code which starts from operatorRegions string array's index
		int jioRange = 8872;
		
		//inserts idea numbers with the region code which starts from operatorRegions string array's index
		int ideaRange = 9814;
		
		//inserts vi numbers with the region code which starts from operatorRegions string array's index
		int viRange = 6800;
		
		prepapredStatement.setInt(1,airtelRange);
		prepapredStatement.setString(2,"Airtel");
		prepapredStatement.addBatch();
		
		prepapredStatement.setInt(1,jioRange);
		prepapredStatement.setString(2,"jio");
		prepapredStatement.addBatch();
		
		prepapredStatement.setInt(1,ideaRange);
		prepapredStatement.setString(2,"Idea");
		prepapredStatement.addBatch();
		
		prepapredStatement.setInt(1,viRange);
		prepapredStatement.setString(2,"VI");
		prepapredStatement.addBatch();
		
		
		
		//executes all statements at once
		prepapredStatement.executeBatch();
		

		log.info("Inserted operator values");
		
		}
		catch(SQLException e) {
			
			//checks if the exception code matches the duplicate inserts in primary key
			if(e.getErrorCode() == 1062) {
				log.info("Duplicate values inserted in operator table");
			}
			//throws any other exception if occurred
			else {
				throw new Exception("Error while inserting in operator table");
			}

		}
		
	}
	
	
	/**
	 * inserts / populates the message table
	 * @param connection -> connection to the database
	 * @param messageTable ->  table name
	 * @throws Exception
	 */
	public static void insertMessageDetails(Connection connection, String messageTable) throws Exception {
		
		//if the passed value from the configs is empty or missing it sets the default value
		if(messageTable == null || messageTable.isEmpty()) {
			log.info("Default message table used");
			messageTable = MESSAGE_TABLE;
		}
		
		//inserting values in tables
		try(Statement statement = connection.createStatement()){
		
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (19, 9872448908, 9872848978, 'Hi ali hows you', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (1, 8872349908, 9872848988, 'we are going to have a problem soon bro', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (2, 6800449908, 8872349908, 'hehehehehhehe', NOW(), NOW(), 'failed')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (3, 6800349908, 8872349908, 'hehehehehhehe', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (4, 9814449908, 6800349908, 'its time to sleep', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (5, 6800449908, 9814009908, 'see you bro there', NOW(), NOW(), 'failed')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (6, 8872049908, 6800849908, 'same time same place', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (7, 8872449908, 8872949908, 'we are going to gym', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (8, 6800349998, 6800949908, 'dont call back ', NOW(), NOW(), 'failed')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (9, 9872449908, 8872349908, 'please call back', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (10, 6800449908, 9872848908, 'we ll probably not finish this by today', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (11, 9872848978, 8872349908, 'we ll do that', NOW(), NOW(), 'failed')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (12, 8872049908, 6800649908, 'thats it for you', NOW(), NOW(), 'Delivered')");
			statement.addBatch("INSERT INTO " + messageTable + " VALUES (13, 8872349908, 9814084897, 'they are late', NOW(), NOW(), 'Delivered')");
	        statement.addBatch("INSERT INTO " + messageTable + " VALUES (14, 6800349976, 9814084897, 'rejected idea', NOW(), NOW(), 'failed')");
	        statement.addBatch("INSERT INTO " + messageTable + " VALUES (15, 9872448978, 8872349908, 'select them all', NOW(), NOW(), 'Delivered')");
	        statement.addBatch("INSERT INTO " + messageTable + " VALUES (16, 8872349908, 6800349908, 'buy everything', NOW(), NOW(), 'Delivered')");
	        statement.addBatch("INSERT INTO " + messageTable + " VALUES (17, 6800349934, 9872848921, 'got it', NOW(), NOW(), 'failed')");
	        statement.addBatch("INSERT INTO " + messageTable + " VALUES (18, 6800349999, 9872348908, 'thats it tired', NOW(), NOW(), 'Delivered')");
	        
	        statement.executeBatch();

	        log.info("Inserted message details values");
			
		}
		
		
		
		catch(SQLException e) {
			//checks if the exception code matches the duplicate inserts in primary key
			if(e.getErrorCode() == 1062) {
				log.info("Duplicate values inserted in message table");
			}
			//throws any other exception if occurred
			else {
				throw new Exception("Error while inserting in message table");
			}

		}
			
		
		
	}
	
	
	
	
	
	
	
	/**
	 * Initialize table for storing operator details
	 * @param connection -> connection to database
	 * @param operatorTable -> table name
	 * @throws Exception
	 */
	public static void operatorRegionTableIntializer(Connection connection, String operatorRegionTable) throws Exception{
		
		//if the passed value from the configs is empty or missing it sets the default value
//		if(operatorTable == null || operatorTable.isEmpty()) {
//			log.info("Default operator table name used");
//			operatorTable = OPERATOR_TABLE;
//		}		
//		
		//creates MESSAGE table with mentioned field
		try(Statement stmt = connection.createStatement();) {		      
		          String create_table = "CREATE TABLE " + operatorRegionTable +
		                   "(region_id INT not NULL, " +
		                   " region VARCHAR(255), " +
		       		       " PRIMARY KEY (region_id ))"; 

		         stmt.executeUpdate(create_table);
		         log.info("Created operator table in given database");   	  
		      } 
		catch (SQLException e) {
			System.out.println(e.getMessage());
			   	  
    	//checks if the exception code matches the table exists code and then logs table exists error
			if(e.getErrorCode() == 1050) {
				log.info(operatorRegionTable+" already exists");
			}
			//throws any other exception if occurred
			else {
				throw new Exception("Error while creating operator table");
			}
	         
		  }
	}
	
	
public static void insertOperatorRegionValue(Connection connection, String operatorRegionTable) throws Exception {
		
		//if the passed value from the configs is empty or missing it sets the default value
//		if(operatorTable == null || operatorTable.isEmpty()) {
//			log.info("Default operator table used");
//			operatorTable = OPERATOR_TABLE;
//		}	
		
		//inserts value in the table and exits if insert errors exists
		try {
		
		//prepared statement to insert in table	
		PreparedStatement prepapredStatement = connection.prepareStatement("INSERT INTO " + operatorRegionTable + " VALUES (?, ?)");
		
		//string array of regions
		String[] operatorRegions = new String[] {"Himachal Pradesh", "Jammu and Kashmir", "Ladakh", "Uttrakhand", "Punjab", "Haryana", "Delhi", "Uttar Pradesh", "Rajasthan", "Bihar"};
		
		//inserts airtel numbers with the region code which starts from operatorRegions string array's index
		for(int i = 0; i < operatorRegions.length; i++) {
			
			prepapredStatement.setInt(1,i);
			prepapredStatement.setString(2, operatorRegions[i]);
			prepapredStatement.addBatch();
			
		}
		
		
		//executes all statements at once
		prepapredStatement.executeBatch();
		

		log.info("Inserted region values");
		
		}
		catch(SQLException e) {
			
			//checks if the exception code matches the duplicate inserts in primary key
			if(e.getErrorCode() == 1062) {
				log.info("Duplicate values inserted in operator table");
			}
			//throws any other exception if occurred
			else {
				throw new Exception("Error while inserting in operator table");
			}

		}
		
	}
	
	
  public static void joinTable(String operatorTable, String messageTable, String region, Connection connection)	{
	  
	  
	  try(Statement stmt = connection.createStatement();) {
		  
		  String join_table = " create table sender_region_join1 as" +
		  					  " select a.*, b.region as sent_region from  message_data as a inner join " + region + " b where FLOOR(((a.SENDER_NUMBER/100000) %10)) = b.region_id;" ;
		  
		  stmt.executeUpdate(join_table);
		  
		  join_table = "create table reciever_region_join1 as" +
		  			   " select a.*, b.region as recieved_region from sender_region_join1 a inner join regioninfo b where FLOOR(((a.reciever_nUMBER/100000) %10)) = b.region_id;";
		  
		  stmt.executeUpdate(join_table);
		  
		  join_table = "create table reciever_operator_join1 as" +
				       " select a.*, b.operator as recieved_operator from reciever_region_join1 a inner join operator_range_data b where FLOOR((a.reciever_nUMBER/1000000)) = b.phone_range/10;";
		  
		  stmt.executeUpdate(join_table);
		  
		  join_table = "create table finalMessageInfo1 as" +
				       " select a.*, b.operator as sender_operator from reciever_operator_join1 a inner join operator_range_data b where FLOOR((a.sender_nUMBER/1000000)) = b.phone_range/10;";
		  
		  stmt.executeUpdate(join_table);
		  
		  
	  } catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  
	  
	  
  }
  
  // a + b + c + d
  // t1 = a+b
//  t2 = t1 + c
//	t4 = 	  t3 + d

 





  public static void drop(Connection connection, String databaseName)throws Exception {
	  
	//creates database with the passed database name
			try(Statement stmt = connection.createStatement()) {		      
		         String createDatabase = "DROP DATABASe " + databaseName;
		         stmt.executeUpdate(createDatabase);
		         log.info("Dropped successfully");   	  
			} 
			catch (SQLException e) {
				
				//checks if the exception code matches the database exists code and logs database exists error
				if(e.getErrorCode() == 1007) {
					log.info("Database already exists");
				}
				//throws any other exception if occurred
				else {
					throw new Exception("Error while creating database");
				}
			} 
	  
  }



	
	//main function
	
	public static void main(String[] args) {
		
		
		try {
			
		//intializes	
		resourceIntializer();
		
		// intializing file path from the property
		//getresourcevalue some better name
		String databaseURL = ReadProperties.getResource("Db_URL");
		String userID = ReadProperties.getResource("User");
		String password = ReadProperties.getResource("Password");
		String databaseName = ReadProperties.getResource("Database");
		String operatorTable = ReadProperties.getResource("Operator_Table");
		String messageTable = ReadProperties.getResource("Message_Table");
		String region = "regionInfo";
	
		//crating connections to database
	    Connection connection = databaseConnector(databaseURL, userID, password);
	    
	    drop(connection, "Mobile_Operator");
		
	    //intialising and selecting the database
		databaseIntializer(connection, databaseName);
		databaseSelector(connection, databaseName);
		
		//intializing tables
		operatorRangeInfoTableIntializer(connection, operatorTable);
		messageInfoTableIntializer(connection, messageTable);
		operatorRegionTableIntializer(connection, region);
		
		//inserting values in tables
		insertOperatorRangeValue(connection, operatorTable);
		insertMessageDetails(connection, messageTable);
		insertOperatorRegionValue(connection, region);
		
		
		
		//joinTable(operatorTable, messageTable, region, connection);
		
		
		
		
		
		//executing query
		Query.queryExceutor(connection);
		
		//closing the connection
		connection.close();
		
		
	}
		catch(Exception e) {
			//connection.close();
			log.error(e.getMessage());
		}
	}
		
		
	}

