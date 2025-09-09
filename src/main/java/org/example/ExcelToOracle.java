package org.example;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;

public class ExcelToOracle {
    public static void main(String[] args) {
        String excelPath = "example.xlsx";
        String jdbcUrl = "jdbc:oracle:thin:@localhost:1521:xe"; // DB 접속 URL
        String dbUser = "scott";   // 사용자명
        String dbPassword = "tiger"; // 비밀번호

        try (FileInputStream fis = new FileInputStream(excelPath);
             Workbook workbook = new XSSFWorkbook(fis);
             Connection conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)) {

            Sheet sheet = workbook.getSheetAt(0);

            // 예: 엑셀 첫 번째 컬럼 값으로 DB 조회
            String sql = "SELECT name, age FROM users WHERE user_id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                for (Row row : sheet) {
                    Cell cell = row.getCell(0); // 첫 번째 셀 값
                    if (cell == null) continue;

                    String userId = cell.getStringCellValue();

                    pstmt.setString(1, userId);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            String name = rs.getString("name");
                            int age = rs.getInt("age");
                            System.out.println("UserId=" + userId + ", Name=" + name + ", Age=" + age);
                        }
                    }
                }
            }

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
}
