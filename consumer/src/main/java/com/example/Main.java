package com.example;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker address
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString()); // Consumer group ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        String topic = "ddia4"; 
        consumer.subscribe(Collections.singletonList(topic));
        ArrayList<Long> latencies = new ArrayList<>();
        try (FileWriter csvWriter = new FileWriter("latencies_consumer_e2e.csv")) {
            csvWriter.append("Run_Number,Latency\n"); 
            int msgCount = 0;
            while(msgCount<10000) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000)); 
                for(ConsumerRecord<String, String> record : records) {
                    msgCount++;
                    long start = Long.parseLong(new String(record.headers().lastHeader("timestamp").value()));
                    long latency = System.currentTimeMillis() - start;
                    latencies.add(latency);
                    csvWriter.append( msgCount + "," + latency + "\n"); 
                }
            }
        }

        latencies.sort(Long::compareTo);
        System.out.println("median latency1:" + latencies.get(latencies.size() / 2));
        System.out.println("median latency1:" + latencies.get((latencies.size() / 2) - 1));
        System.out.println("median latency avg:" + (double) (latencies.get(latencies.size() / 2) + latencies.get((latencies.size() / 2) - 1)) / 2);

        consumer.close();

    }
}