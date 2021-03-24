package com.acme.eshop;

import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
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

		// Iitial load of reference tables
		if (demo.createTable()) {
			demo.insertRefTables();
		}

		//BASIC FUNCTIONALITY-STEP1
		//LOAD PRODUCTS IN ORDER TO BE SEEN BY THE USERS
		demo.loadProducts();

		//A NEW CUSTOMER MAKES AN ACCOUNT
		demo.CreateNewCustomer();

		//A CUSTOMER MAKES AN ORDER,DISCOUNT IS CALCULATED AND AFTER THE PAYMENT IS DONE ORDER IS FINALIZED
		demo.CheckPe();
		demo.CreateOrder();
		demo.GetDiscount();
		demo.FinalizeOrder();

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

	private boolean createTable() {
		try (Statement statement = ConnectionPoolProvider.getConnection().createStatement()) {
			int resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.001"));
			resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.002"));
			resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.003"));
			resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.004"));
			resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.005"));
			resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.006"));
			resultRows = statement.executeUpdate(sqlCommands.getProperty("create.table.007"));

			logger.debug("Tables created {}.", resultRows);
			return true;
		} catch (SQLException throwables) {
			logger.debug("Error creating Tables:", throwables);
			return false;
		}
	}

	private void insertRefTables() {
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
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.007"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.008"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.009"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.010"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.011"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.012"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.013"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.014"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.015"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.016"));
			logger.debug("Statement returned {}.", resultRows);

		} catch (SQLException throwables) {
			logger.error("Error occurred while inserting data.", throwables);
		}
	}

	private void loadProducts() {
		try (Statement statement = ConnectionPoolProvider.getConnection().createStatement();
			 ResultSet resultSet = statement.executeQuery(sqlCommands.getProperty("select.table.003"))) {

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

	private void CreateNewCustomer() {
		try (PreparedStatement preparedStatement = ConnectionPoolProvider.getConnection().prepareStatement(
				sqlCommands.getProperty("insert.table.000"))) {
			generateData(preparedStatement, 1);

			int[] affectedRows = preparedStatement.executeBatch();
			logger.debug("Rows inserted {}.", Arrays.stream(affectedRows).sum());

		} catch (SQLException throwables) {
			logger.error("Error occurred while batch inserting data.", throwables);
		}
	}

	private void generateData(PreparedStatement preparedStatement, int howMany) throws SQLException {
		for (int i = 1; i <= howMany; i++) {
			preparedStatement.clearParameters();

			preparedStatement.setLong(1, 1005 + i);
			preparedStatement.setString(2, generator.getFirstName());
			preparedStatement.setString(3, generator.getLastName());
			preparedStatement.setString(4, generator.getEmail());
			preparedStatement.addBatch();
		}
	}

	private void CheckPe() {
		try (Statement statement = ConnectionPoolProvider.getConnection().createStatement();
			 ResultSet resultSet = statement.executeQuery(sqlCommands.getProperty("select.table.001"))) {

			while (resultSet.next()) {
				//@formatter:off
				logger.error("A Pending order exists for the customer");
				//@formatter:on
			}
		} catch (SQLException throwables) {
			logger.error("Error occurred while retrieving products", throwables);
		}
	}

	private void CreateOrder() {
		try (Statement statement = ConnectionPoolProvider.getConnection().createStatement()) {
			int resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.017"));
			logger.debug("Statement returned {}.", resultRows);
			resultRows = statement.executeUpdate(sqlCommands.getProperty("insert.table.018"));
			logger.debug("Statement returned {}.", resultRows);
		} catch (SQLException throwables) {
			logger.error("Error occurred while inserting data.", throwables);
		}
	}

	private void GetDiscount() {
		try (Statement statement = ConnectionPoolProvider.getConnection().createStatement();
			 ResultSet resultSet = statement.executeQuery(sqlCommands.getProperty("select.table.002"))) {

			while (resultSet.next()) {
				//@formatter:off
				logger.info("C1:{}", resultSet.getInt("C1"));
				//@formatter:on
			}
		} catch (SQLException throwables) {
			logger.error("Error occurred while retrieving products", throwables);
		}
	}

	private void FinalizeOrder() {
		try (Statement statement = ConnectionPoolProvider.getConnection().createStatement()) {
			int resultRows = statement.executeUpdate(sqlCommands.getProperty("update.table.001"));

			logger.debug("Rows updated {}.", resultRows);

		} catch (SQLException throwables) {
			logger.error("Error occurred while updating data.", throwables);
		}
	}

}
