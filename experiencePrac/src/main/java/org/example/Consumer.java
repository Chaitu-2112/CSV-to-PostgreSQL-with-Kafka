package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.sql.*;
import java.time.Duration;
import java.util.*;

public class Consumer {

    public static void main(String[] args) {
        String topic = "employee";

        // Kafka consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "recvue-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        String jdbcURL = "jdbc:postgresql://localhost:5432/kafka";
        String username = "postgres";
        String password = "Chaitu@2112";

        System.out.println("Consumer started for topic '" + topic + "'...");

        try (Connection conn = DriverManager.getConnection(jdbcURL, username, password)) {
            conn.setAutoCommit(false);

            // --- STEP 1: Fetch last committed offsets from DB ---
            Map<TopicPartition, Long> partitionOffsets = new HashMap<>();

            String sql = "SELECT partition, last_offset FROM kafka_offsets WHERE topic = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, topic);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    int partition = rs.getInt("partition");
                    long offset = rs.getLong("last_offset");
                    TopicPartition tp = new TopicPartition(topic, partition);
                    partitionOffsets.put(tp, offset);
                    System.out.println("Found stored offset " + offset + " for partition " + partition);
                }
            }

            if (!partitionOffsets.isEmpty()) {
                consumer.assign(partitionOffsets.keySet());
                for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
                    consumer.seek(entry.getKey(), entry.getValue() + 1);
                    System.out.println("Resuming from offset " + (entry.getValue() + 1) +
                            " for partition " + entry.getKey().partition());
                }
            } else {
                consumer.subscribe(Collections.singletonList(topic));
                System.out.println("No previous offsets found. Subscribed normally.");
            }

            int recordCounter = 0;

            // --- STEP 2: Start polling ---
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    System.out.println("No records polled. Waiting...");
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    recordCounter++;
                    System.out.println("Consuming record #" + recordCounter + ": " + record.value());

                    try {
                        JSONObject json = new JSONObject(record.value());
                        int id = json.getInt("id");
                        String name = json.getString("name");
                        String dept = json.getString("department");
                        double salary = json.getDouble("salary");

                        // Insert / Update employee
                        String empSql = "INSERT INTO employees (id, name, department, salary) " +
                                "VALUES (?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET " +
                                "name = EXCLUDED.name, department = EXCLUDED.department, salary = EXCLUDED.salary";

                        try (PreparedStatement stmt = conn.prepareStatement(empSql)) {
                            stmt.setInt(1, id);
                            stmt.setString(2, name);
                            stmt.setString(3, dept);
                            stmt.setDouble(4, salary);
                            stmt.executeUpdate();
                        }

                        // Save offset in kafka_offsets
                        String offsetSql = "INSERT INTO kafka_offsets (topic, partition, last_offset) " +
                                "VALUES (?, ?, ?) ON CONFLICT (topic, partition) " +
                                "DO UPDATE SET last_offset = EXCLUDED.last_offset";

                        try (PreparedStatement stmt = conn.prepareStatement(offsetSql)) {
                            stmt.setString(1, record.topic());
                            stmt.setInt(2, record.partition());
                            stmt.setLong(3, record.offset());
                            stmt.executeUpdate();
                        }

                        conn.commit();

                        System.out.println("Inserted/Updated employee: " + name +
                                " | Saved offset: " + record.offset());

                        // 🚨 Stop after 4th record
                        if (recordCounter == 4) {
                            System.out.println("Stopping consumer after processing 4 records...");
                            consumer.close();
                            return; // exit program
                        }

                    } catch (SQLException e) {
                        System.err.println("DB failure: " + e.getMessage() + ". Rolling back...");
                        conn.rollback();
                        Thread.sleep(3000);
                        break;
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
