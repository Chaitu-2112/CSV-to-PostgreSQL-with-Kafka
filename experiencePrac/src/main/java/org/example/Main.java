package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        String topic = "employee";
        String filePath = "src/main/java/org/example/Employee.csv";

        // Kafka producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader br = new BufferedReader(new FileReader(filePath))) {

            // Read header (first row of CSV)
            String headerLine = br.readLine();
            if (headerLine == null) {
                System.err.println("CSV file is empty!");
                return;
            }
            String[] headers = headerLine.split(",");

            String line;
            int recordCount = 0;

            // Read and send each line as a JSON message
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                JSONObject json = new JSONObject();

                for (int i = 0; i < headers.length; i++) {
                    json.put(headers[i].trim(), values[i].trim());
                }

                // Use "id" as Kafka key for partition consistency
                String key = json.optString("id", String.valueOf(recordCount));

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, key, json.toString());

                // Send asynchronously with callback
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Sent record [key=%s] -> partition=%d, offset=%d%n",
                                key, metadata.partition(), metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });

                recordCount++;
            }

            producer.flush();
            System.out.println("✅ Finished sending " + recordCount + " records to topic '" + topic + "'.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
