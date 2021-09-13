package com.github.maneesh.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger =  LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServer = "127.0.0.1:9092";

        Properties properties =  new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer =  new KafkaProducer<String, String>(properties);

        for(int i=0; i<10; i++){
            String topic = "first_topic";
            String value = "Hello World from Java with keys! " + i;
            String key = "id_" + i;
            ProducerRecord<String,String> record = new ProducerRecord(topic, key,value);

            logger.info("Key: "+key); //log the key

            kafkaProducer.send(record, (recordMetadata, e) -> {
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
            }).get();

        }

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
