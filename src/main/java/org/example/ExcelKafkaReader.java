package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class ExcelKafkaReader {
    public static void main(String[] args) {
        String excelPath = "example.xlsx";
        String topic = "my-topic";
        int partition = 0;
        long startOffset = 100L;  // 원하는 오프셋

        // 1. 엑셀에서 값 읽기
        List<String> conditions = readExcelColumnValues(excelPath, 0); // 0번째 열 읽기

        // 2. Kafka Consumer 설정
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "excel-offset-reader");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(Collections.singletonList(topicPartition));
            consumer.seek(topicPartition, startOffset);

            System.out.println("=== 조건: " + conditions + " | Offset 시작: " + startOffset + " ===");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value();

                    // 엑셀 값이 포함된 메시지만 출력
                    for (String cond : conditions) {
                        if (value != null && value.contains(cond)) {
                            System.out.printf("조건[%s] 매칭 | partition=%d, offset=%d, key=%s, value=%s%n",
                                    cond, record.partition(), record.offset(), record.key(), value);
                        }
                    }
                }
            }
        }
    }

    // 엑셀 첫 번째 시트, 지정한 컬럼의 값 읽기
    private static List<String> readExcelColumnValues(String filePath, int columnIndex) {
        List<String> values = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(filePath);
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            for (Row row : sheet) {
                Cell cell = row.getCell(columnIndex);
                if (cell != null && cell.getCellType() == CellType.STRING) {
                    values.add(cell.getStringCellValue().trim());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return values;
    }
}

