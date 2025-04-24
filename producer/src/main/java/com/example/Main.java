package com.example;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        String msg = Files.readString(Path.of("producer/src/main/resources/message.txt"));

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        ArrayList<Long> latencies = new ArrayList<>();
        ArrayList<Future<RecordMetadata>> futures = new ArrayList<>();
        try(Producer<String, String> producer = new KafkaProducer<>(props)){
            FileWriter csvWriter = new FileWriter("latencies_producer.csv");
            ProducerRecord<String, String> record = new ProducerRecord<>("ddia4", null, msg);
            csvWriter.append("Run_Number,Latency\n");
            for (int i = 0; i < 1000; i++) {
                long start = System.currentTimeMillis();
                for (int j = 0; j < 1000; j++) {
                    futures.add(producer.send(record));
                }
                for (Future<RecordMetadata> future : futures) {
                    try {
                        future.get();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                long latency =System.currentTimeMillis() - start ;
                csvWriter.append(i + 1 + "," + latency + "\n");
                latencies.add(latency);
                futures.clear();
            }
            csvWriter.close();
        }

        
        latencies.sort(Long::compareTo);
        System.out.println("median latency1:" + latencies.get(latencies.size() / 2));
        System.out.println("median latency1:" + latencies.get((latencies.size() / 2)-1));
        System.out.println("median latency avg:" + (double)(latencies.get(latencies.size() / 2)+latencies.get((latencies.size() / 2)-1))/2);

    }
}