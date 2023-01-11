package com.asirtech.kafkaconsumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asirtech.kafkaconsumer.DTO.StudentModel;

public class DataProvider {
	
	private static final Logger logger = LoggerFactory.getLogger(DataProvider.class);
	private int i=0, j=1 ;
	public void dataProvider(String tablename, StudentModel value, Connection con) {
		
//		String sq = new StringBuilder()
//				.append("INSERT INTO ")
//				.append(tablename)
//				.append("(reg_no, first_name, last_name, ")
//				.append("location, state, language) VALUES('")
//				.append(value.getReg_no()+"','")
//				.append(value.getFirst_name()+"','")
//				.append(value.getLast_name()+"','")
//				.append(value.getLocation()+"','")
//				.append(value.getState()+"','")
//				.append(value.getLanguage()+"');")
//				.toString();
		
		String query = "INSERT INTO "+tablename+" (reg_no, first_name, last_name, location, state, language) "
							+ "VALUES(?, ?, ?, ?, ?, ?);";
		try {
			PreparedStatement pstate = con.prepareStatement(query) ;
			pstate.setInt(1, value.getReg_no());
			pstate.setString(2, value.getFirst_name());
			pstate.setString(3, value.getLast_name());
			pstate.setString(4, value.getLocation());
			pstate.setString(5, value.getState());
			pstate.setString(6, value.getLanguage());
			System.out.println("Query is -> " + query);
			i = pstate.executeUpdate();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		
		
		/*String sql = "INSERT INTO "+tablename+"(reg_no, first_name, last_name, "
							+ "location, state, language) VALUES("
							+ "'"+value.getReg_no()+"','"
							+ value.getFirst_name()+"','"
							+ value.getLast_name()+"','"
							+ value.getLocation()+"','"
							+ value.getState()+"','"
							+ value.getLanguage()+"');";
		*/
//		System.out.println("The Query is => " + sq);
//		Statement statement;
//		try {
//			statement = con.createStatement();
//			i = statement.executeUpdate(sq);
//			
//			statement.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		if(i==1) {
			System.out.println("Sno "+ j +": The values inserted successfully!!...");
			logger.info("Values Inserted... from consumer record");
			j++;
		}
	}
}
