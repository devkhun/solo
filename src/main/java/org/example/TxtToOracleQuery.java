package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;

public class TxtToOracleQuery {
    public static void main(String[] args) {
        String txtFile = "data.txt"; // 프로젝트 루트에 data.txt 저장
        String jdbcUrl = "jdbc:oracle:thin:@localhost:1521:xe";
        String username = "your_user";
        String password = "your_pass";

        try (BufferedReader br = new BufferedReader(new FileReader(txtFile));
             Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {

            String line;
            boolean firstLine = true;

            while ((line = br.readLine()) != null) {
                if (firstLine) { // 헤더(id,time,code) 건너뛰기
                    firstLine = false;
                    continue;
                }

                String[] cols = line.split(","); // 쉼표 기준 split
                if (cols.length < 3) continue;

                String id = cols[0].trim();
                String time = cols[1].trim();
                String code = cols[2].trim();

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
                            System.out.println("결과: " +
                                    rs.getString("id") + ", " +
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
