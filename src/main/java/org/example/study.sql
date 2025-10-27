-- 오라클 버전
SELECT
    eqp_id,
    SUBSTR(timekey, 1, 12) AS timekey_minute,
    COUNT(*) AS cnt
FROM your_table
GROUP BY eqp_id, SUBSTR(timekey, 1, 12)
ORDER BY eqp_id, timekey_minute;

-- mysql 버전
SELECT
    eqp_id,
    LEFT(timekey, 12) AS timekey_minute,
    COUNT(*) AS cnt
FROM your_table
GROUP BY eqp_id, LEFT(timekey, 12)
ORDER BY eqp_id, timekey_minute;
