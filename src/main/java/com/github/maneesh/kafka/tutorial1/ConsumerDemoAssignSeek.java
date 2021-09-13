package com.github.maneesh.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";
        String groupId = "my-fourth-application";

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // Assign and Seek are mostly used to replay a data or fetch a specefic message

        //assign
        TopicPartition partitionToReadFrom =  new TopicPartition(topic,0);
        long offsetToReadFrom = 10;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numOfMsgToRead = 5;
        boolean keepOnReading = true;
        int numOfMsgReadSoFar = 0;

        while (keepOnReading){
            ConsumerRecords<String, String > records =  consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord record : records){
                logger.info("key: "+record.key()+" value: "+ record.value()+ " offset: "+record.offset()
                        +" partition: "+ record.partition());
                numOfMsgReadSoFar++;
                if(numOfMsgReadSoFar >= numOfMsgToRead){
                    keepOnReading = false;
                    break;
                }
            }

        }


    }
}

