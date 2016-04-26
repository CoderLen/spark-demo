package com.liny.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestJDBC {

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String url = "jdbc:oracle:thin:@10.201.26.182:1521:sda";
			String username = "system";
			String password = "systex";
			Connection conn = DriverManager.getConnection(url, username, password);
			Statement st = conn.createStatement();
			System.out.println("连接成功，"+conn.getSchema());
		} catch (Exception e) {

			e.printStackTrace();
		}
	}
}
