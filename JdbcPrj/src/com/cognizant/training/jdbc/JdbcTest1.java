package com.cognizant.training.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcTest1 {
public static void main(String[] args) {
        try {
                Connection con=DriverManager.getConnection("jdbc:mysql://localhost:3306/trainingdb", "root",
                                "Password@1");
                Statement st=con.createStatement();
                ResultSet rs=st.executeQuery("select * from customer");
                while(rs.next()) {
                        System.out.println(rs.getInt(1)+"\t"+rs.getString(2)+"\t"+rs.getString(3));
                }
                con.close();
        } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
        }
        
}
}



