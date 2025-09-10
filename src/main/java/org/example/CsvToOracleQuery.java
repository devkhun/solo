package org.example;

import com.opencsv.CSVReader;
import java.io.FileReader;
import java.sql.*;
import java.util.*;

public class CsvToOracleQuery {
    public static void main(String[] args) {
        String csvFile = "data.txt"; // 같은 프로젝트 경로에 두면 됨
        String jdbcUrl = "jdbc:oracle:thin:@localhost:1521:xe";
        String username = "your_user";
        String password = "your_pass";

        try (CSVReader reader = new CSVReader(new FileReader(csvFile));
             Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {

            // CSV 첫 줄은 헤더라서 skip
            String[] nextLine = reader.readNext();

            while ((nextLine = reader.readNext()) != null) {
                String id = nextLine[0];
                String time = nextLine[1];   // 20250910125959
                String code = nextLine[2];

                // SQL 조건 생성 (time은 YYYYMMDDHH24MISS → DATE 변환)
                String sql = "SELECT * FROM your_table " +
                        "WHERE id = ? " +
                        "AND code = ? " +
                        "AND time_column = TO_DATE(?, 'YYYYMMDDHH24MISS')";

                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, id);
                    pstmt.setString(2, code);
                    pstmt.setString(3, time);

                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            System.out.println("결과: " + rs.getString("id") + ", " +
                                    rs.getString("code") + ", " +
                                    rs.getString("time_column"));
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
