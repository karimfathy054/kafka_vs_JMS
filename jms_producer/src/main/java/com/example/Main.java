package com.example;


import org.apache.activemq.ActiveMQConnectionFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

public class Main {

    public static void main(String[] args) {

        try {
            String messageContent = Files.readString(Path.of("/home/karim/ddia labs/ddia 4/message.txt"));

            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Destination destination = session.createQueue("JMS e2e test");

            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            
            ArrayList<Long> latencies = new ArrayList<>();
            // FileWriter csvWriter = new FileWriter("JMS_latencies_producer.csv");
            // csvWriter.append("Run_number,Latency\n");
            for (int i = 0; i < 10000; i++) {
                long start = System.currentTimeMillis();
                // for (int j = 0; j < 1000; j++) {
                    TextMessage message = session.createTextMessage(messageContent);
                    message.setLongProperty("Timestamp", System.currentTimeMillis());
                    producer.send(message);
                // }
                long latency = System.currentTimeMillis() - start;

                latencies.add(latency);
                // csvWriter.append((i + 1) + "," + latency + "\n");
            }

            latencies.sort(Long::compareTo);
            System.out.println("median latency1:" + latencies.get(latencies.size() / 2));
            System.out.println("median latency1:" + latencies.get((latencies.size() / 2) - 1));
            System.out.println("median latency avg:" + (double) (latencies.get(latencies.size() / 2) + latencies.get((latencies.size() / 2) - 1)) / 2);


            producer.close();
            session.close();
            connection.close();
            // csvWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}