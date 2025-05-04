package com.example;

import java.io.FileWriter;
import java.util.ArrayList;

import org.apache.activemq.ActiveMQConnectionFactory;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
public class Main {
    public static void main(String[] args) {
        try {

            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Destination destination = session.createQueue("JMS e2e test");

            MessageConsumer consumer = session.createConsumer(destination);


            ArrayList<Long> latencies = new ArrayList<>();
            FileWriter csvWriter = new FileWriter("JMS_latencies_e2e.csv");
            csvWriter.append("Run_number,Latency\n");
            // for (int i = 0; i < 1000; i++) {
            int msgCount = 0;
            long latency = 0 ;
            while(msgCount<10000){
                Message m = consumer.receive(1000000);
                if (m instanceof TextMessage){
                    latency =System.currentTimeMillis()-m.getLongProperty("Timestamp"); 
                }
                msgCount++;
                latencies.add(latency);
                csvWriter.append((msgCount+1)+","+latency+"\n");
            }
            // }

            latencies.sort(Long::compareTo);
            System.out.println("median latency1:" + latencies.get(latencies.size() / 2));
            System.out.println("median latency1:" + latencies.get((latencies.size() / 2) - 1));
            System.out.println("median latency avg:" + (double) (latencies.get(latencies.size() / 2) + latencies.get((latencies.size() / 2) - 1)) / 2);


            consumer.close();
            session.close();
            connection.close();
            csvWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
