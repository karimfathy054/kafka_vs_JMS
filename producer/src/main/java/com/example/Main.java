package com.example;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        String msg = Files.readString(Path.of("producer/src/main/resources/message.txt"));

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        try(Producer<Long, String> producer = new KafkaProducer<>(props)){
            for (int i = 0; i < 10000; i++) {
                long start = System.currentTimeMillis();
                RecordHeader timeStampHeader = new RecordHeader("timestamp", String.valueOf(start).getBytes());
                ProducerRecord<Long, String> record = new ProducerRecord<>("ddia4", null,null, msg,List.of(timeStampHeader));
                producer.send(record);
            }
        }
    }
}