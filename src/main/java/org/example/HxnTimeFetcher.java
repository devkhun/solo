package org.example;

import java.sql.*;
import java.util.*;

public class HxnTimeFetcher {

    private final Connection conn;

    public HxnTimeFetcher(Connection conn) {
        this.conn = conn;
    }

    public static class HxnRecord {
        String hxnId;
        String event1Time;
        String event2Time;

        @Override
        public String toString() {
            return hxnId + ", "
                    + hxnId + "-1-" + event1Time + ", "
                    + hxnId + "-2-" + event2Time;
        }
    }

    public List<HxnRecord> fetchData() throws SQLException {
        // 1. hxn_id 목록 조회
        String sqlHxn = "SELECT DISTINCT hxn_id FROM hxn_table";
        PreparedStatement ps1 = conn.prepareStatement(sqlHxn);
        ResultSet rs1 = ps1.executeQuery();

        List<String> hxnIds = new ArrayList<>();
        while (rs1.next()) {
            hxnIds.add(rs1.getString("hxn_id"));
        }
        rs1.close();
        ps1.close();

        // 2. 각 hxn_id에 대해 event_cd별 시간 조회
        String sqlTime = """
            SELECT hxn_id, event_cd, time_key 
            FROM event_table 
            WHERE hxn_id = ?
        """;

        PreparedStatement ps2 = conn.prepareStatement(sqlTime);
        List<HxnRecord> result = new ArrayList<>();

        for (String id : hxnIds) {
            ps2.setString(1, id);
            ResultSet rs2 = ps2.executeQuery();

            HxnRecord rec = new HxnRecord();
            rec.hxnId = id;

            while (rs2.next()) {
                String eventCd = rs2.getString("event_cd");
                String timeKey = rs2.getString("time_key");

                if ("1".equals(eventCd)) rec.event1Time = timeKey;
                else if ("2".equals(eventCd)) rec.event2Time = timeKey;
            }

            result.add(rec);
            rs2.close();
        }

        ps2.close();
        return result;
    }

    public static void main(String[] args) throws Exception {
        // 연결 예시
        Connection conn = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/testdb", "user", "pass"
        );

        HxnTimeFetcher fetcher = new HxnTimeFetcher(conn);
        List<HxnRecord> data = fetcher.fetchData();

        for (HxnRecord r : data) {
            System.out.println(r);
        }

        conn.close();
    }
}
