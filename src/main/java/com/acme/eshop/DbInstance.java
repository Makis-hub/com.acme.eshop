package com.acme.eshop;

import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import static java.lang.System.exit;

public class DbInstance {
	private static final Logger logger = LoggerFactory.getLogger(DbInstance.class);
	private Server h2Server, webServer;

	public void startH2Server() {
		try {
			h2Server = Server.createTcpServer("-tcpAllowOthers", "-tcpDaemon");
			h2Server.start();
			webServer = Server.createWebServer("-webAllowOthers", "-webDaemon");
			webServer.start();
			logger.info("H2 Database server is now accepting connections.");
		} catch (SQLException throwables) {
			logger.error("Unable to start H2 database server.", throwables);
			exit(-1);
		}
		logger.info("H2 server has started with status '{}'.", h2Server.getStatus());
	}

	public void stopH2Server() {
		if (h2Server == null || webServer == null) {
			return;
		}

		if (h2Server.isRunning(true)) {
			h2Server.stop();
			h2Server.shutdown();
		}
		if (webServer.isRunning(true)) {
			webServer.stop();
			webServer.shutdown();
		}
		logger.info("H2 Database server has been shutdown.");
	}
}
