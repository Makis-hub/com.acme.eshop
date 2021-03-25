package com.acme.eshop;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPoolProvider {
	private static final String DB_URL = "jdbc:h2:~/sample";
	private static final String DB_USERNAME = "sa";
	private static final String DB_PASSWORD = "";
	private static final String DB_DRIVER_NAME = "org.h2.Driver";

	private static final HikariDataSource hikariDatasource;
	private static final HikariConfig config = new HikariConfig();

	static {
		config.setDriverClassName(DB_DRIVER_NAME);
		config.setJdbcUrl(DB_URL);
		config.setUsername(DB_USERNAME);
		config.setPassword(DB_PASSWORD);

		config.setConnectionTimeout(10000);
		config.setIdleTimeout(600000);
		config.setMaxLifetime(1800000);
		config.setMinimumIdle(10);
		config.setMaxLifetime(100);
		config.setAutoCommit(true);

		config.addDataSourceProperty("cachePrepStmts", "true");
		config.addDataSourceProperty("prepStmtsCacheSize", "500");
		hikariDatasource = new HikariDataSource(config);
	}

	public ConnectionPoolProvider() {
	}

	public static Connection getConnection() throws SQLException {
		return hikariDatasource.getConnection();
	}
}
