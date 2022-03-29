package javadatabases;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Query {

	// static variables accessed as global variables
	public static Logger logger = LogManager.getLogger(OperatorDatabase.class.getName());
	

	/**
	 * prints query for the connection
	 * 
	 * @param connection - connection to the object
	 * @param query      -> query string
	 */
	public static void printQuery(Connection connection, String query) {

		// query results
		try (Statement stmt = connection.createStatement()) {

			ResultSet resultset = stmt.executeQuery(query);

			// querying prints the query
			logger.info("executing query : " + query);

			while (resultset.next()) {
				// Display values
				logger.info("messages: " + resultset.getString("message"));
			}

		}

		catch (SQLException e) {
			logger.error("Error while executing query " + query);
			System.out.println(e);
		}

	}

	
	/**
	 * function to execute queries
	 * 
	 * @param connection -> connects to the object
	 */
	public static void queryExceutor(Connection connection) {

		try (Scanner sc = new Scanner(System.in);) {
			int userChoice;

			do {

				System.out.println("Enter the number to execute following queries : ");
				System.out.println("-----------------------------");
				System.out.println("1. Enter 1 to Print all messages sent from a given number. ");
				System.out.println("2. Enter 2 to Print all messages to a given number. ");
				System.out.println("3. Enter 3 to Print all messages sent between two years. ");
				System.out.println("4. Enter 4 to Print all messages receieved by given number from punjab number. ");
				System.out.println("5. Enter 5 to Print all messages receieved by given number from airtel punjab number. ");
				System.out.println("6. Enter 6 to Print all messages sent by 98786912**, (Where ** could be any two digits). ");
				System.out.println("7. Enter 7 to Print all messages sent from punjab but failed . ");
				System.out.println("8. Enter 0 to exit. ");
				System.out.println("-----------------------------");
				userChoice = sc.nextInt();

				if (userChoice == 1) {

					System.out.println("enter senders number : ");
					BigInteger bigInteger;
					bigInteger = sc.nextBigInteger();
					String query1 = "Select MESSAGE FROM MESSAGE_DATA WHERE SENDER_NUMBER = " + bigInteger;
					// sample input -> 8872049908

					printQuery(connection, query1);

				} else if (userChoice == 2) {

					System.out.println("Enter recieve's number : ");

					BigInteger bigInteger;
					bigInteger = sc.nextBigInteger();

					// sample input -> 9872848978
					String query2 = "Select MESSAGE FROM MESSAGE_DATA WHERE RECIEVER_NUMBER = " + bigInteger;
					printQuery(connection, query2);

				} else if (userChoice == 3) {
					System.out.println("Enter the year range : ");
					int year1 = sc.nextInt();
					int year2 = sc.nextInt();

					// sample input year1 -> 2019 and year2 -> 2022
					String query3 = "Select MESSAGE FROM MESSAGE_DATA WHERE YEAR(SENT_TIME) BETWEEN " + year1 + " AND "
							+ year2;
					printQuery(connection, query3);

				} else if (userChoice == 4) {
					System.out.println("Enter the reciever number : ");

					BigInteger bigInteger;
					bigInteger = sc.nextBigInteger();

					// sample reciever number -> 9872848978

					String query4 = "Select A.MESSAGE FROM MESSAGE_DATA A INNER JOIN REGIONINFO b on FLOOR(((a.SENDER_NUMBER/100000) %10)) = b.region_id "
							+ "WHERE a.RECIEVER_NUMBER = " + bigInteger + " and b.region = 'Punjab'";

					printQuery(connection, query4);

				} else if (userChoice == 5) {

					System.out.println("Enter the reciever number : ");

					BigInteger bigInteger;
					bigInteger = sc.nextBigInteger();

					// sample reciver number -> 8872349908

					String query5 = "select a.message FROM MESSAGE_DATA A INNER JOIN REGIONINFO B ON FLOOR(((A.SENDER_NUMBER/100000) %10)) = B.REGION_ID "
							+ "inner join operator_range_data c on FLOOR(a.SENDER_NUMBER/1000000) = c.phone_range where a.RECIEVER_NUMBER = "
							+ bigInteger + " and c.operator = 'Airtel'" + "and b.region = 'Punjab'";

					printQuery(connection, query5);

				} else if (userChoice == 6) {

					System.out.println("Enter the sender number : ");

					BigInteger bigInteger;
					bigInteger = sc.nextBigInteger();

					// sample input -> 8872349908

					String query6 = "Select MESSAGE FROM MESSAGE_DATA WHERE FLOOR(RECIEVER_NUMBER/100) = 98728489"
							+ " and sender_number = " + bigInteger;

					printQuery(connection, query6);

				} else if (userChoice == 7) {

					String query7 = "Select A.MESSAGE FROM MESSAGE_DATA A INNER JOIN REGIONINFO b on FLOOR(((a.SENDER_NUMBER/100000) %10)) = b.region_id "
							+ "WHERE a.status = 'failed' and b.region = 'Punjab'";

					printQuery(connection, query7);

				} else if (userChoice == 0) {
					System.out.println("You have entered 0, Exiting");
					break;
				} else {
					System.out.println("You have entered " + userChoice + " which is a wrong input.");
				}

			} while (true);

		}
	}
}
