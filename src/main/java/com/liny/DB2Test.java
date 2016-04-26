package com.liny;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class DB2Test {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		 Class.forName("com.ibm.db2.jcc.DB2Driver");
		 String driveUrl = "jdbc:db2://10.201.26.41:50000/test";
	     Connection conn = DriverManager.getConnection(driveUrl,"db2inst", "systex");
	     if(conn!= null){
	    	 System.out.println("连接成功");
	     }else{
	    	 System.out.println("连接失败"); 
	     }
	}
}
