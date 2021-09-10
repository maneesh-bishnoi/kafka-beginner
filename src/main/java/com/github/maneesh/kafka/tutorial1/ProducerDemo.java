package com.github.maneesh.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello from java");
        Logger logger =  LoggerFactory.getLogger(ProducerDemo.class);
        String bootstrapServer = "127.0.0.1:9092";
        Properties properties =  new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer =  new KafkaProducer<String, String>(properties);

        ProducerRecord<String,String> record = new ProducerRecord("first_topic", "Hello World from Java with callback!");

        kafkaProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    logger.info("Record Successfully produced: \n"+
                                    "Topic: "+recordMetadata.topic()+"\n"+
                            "Offset: "+recordMetadata.offset()+"\n"+
                            "Partition: "+recordMetadata.partition()+"\n"+
                            "Timestamp:"+recordMetadata.partition());
                }
                else {
                    logger.error("Error while producing "+e);
                }
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
