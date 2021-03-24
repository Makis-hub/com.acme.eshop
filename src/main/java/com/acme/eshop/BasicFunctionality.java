package com.acme.eshop;

import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static java.lang.System.exit;

public class BasicFunctionality {
	private static final Logger logger = LoggerFactory.getLogger(BasicFunctionality.class);
	private final Properties sqlCommands = new Properties();
	private final Lorem generator = LoremIpsum.getInstance();

	public static void main(String[] args) throws InterruptedException {
		BasicFunctionality demo = new BasicFunctionality();
		// Load SQL commands
		demo.loadSqlCommands();

		DbInstance dbInstance = new DbInstance();
		// Start H2 database server
		dbInstance.startH2Server();

		// Create tables of the project
		demo.createTable();

		//BASIC FUNCTIONALITY-STEP1
		//CREATE A LIST OF  PRODUCTS
		demo.insertProducts();
		//LOAD PRODUCTS IN ORDER TO BE SEEN BY THE USERS
		demo.loadProducts();

		//A NEW CUSTOMER MAKES AN ACCOUNT
		//demo.insertNewCustomer();

		// Stop H2 database server via shutdown
		Runtime.getRuntime().addShutdownHook(new Thread(() -> dbInstance.stopH2Server()));

		while (true) {
		}
	}

	private void loadSqlCommands() {
		try (InputStream inputStream = BasicFunctionality.class.getClassLoader().getResourceAsStream(
				"sql.properties")) {
			if (inputStream == null) {
				logger.error("Unable to load SQL commands.");
				exit(-1);
			}
			sqlCommands.load(inputStream);
		} catch (IOException e) {
			logger.error("Error while loading SQL commands.", e);
		}
	}

	private void createTable() {
		try (Statement statement = ConnectionPoolProvider.getConnection().createStatement()) {
			int resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.001"));
			resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.002"));
			resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.003"));
			resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.004"));
			resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.005"));
			resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.006"));

			logger.debug("Tables created {}.", resultRows);
		} catch (SQLException throwables) {
			logger.error("Error creating Tables:", throwables);
		}
	}

	private void insertProducts() {
		try (Statement statement = ConnectionPoolProvider.getConnection().createStatement()) {
			int resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.001"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.002"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.003"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.004"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.005"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.006"));
			logger.debug("Statement returned {}.", resultRows);

		} catch (SQLException throwables) {
			logger.error("Error occurred while inserting data.", throwables);
		}
	}

	private void loadProducts() {
		try (Statement statement = ConnectionPoolProvider.getConnection().createStatement();
			 ResultSet resultSet = statement.executeQuery(sqlCommands.getProperty("select.table.001"))) {

			while (resultSet.next()) {
				//@formatter:off
				logger.info("PRODID:{}, PRODDESCR:{}, PRICE:{}", resultSet.getLong("PRODID"),
							resultSet.getString("PRODDESCR"), resultSet.getBigDecimal("PRICE"));
				//@formatter:on
			}
		} catch (SQLException throwables) {
			logger.error("Error occurred while retrieving products", throwables);
		}
	}

}
