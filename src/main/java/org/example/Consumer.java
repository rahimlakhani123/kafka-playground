package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "rw.kfc2u5bg6bemethphddq.at.double.cloud:9091");

        // Configure SASL/PLAIN authentication
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"admin\" " +
                        "password=\"yOV1oapSXmXSeCDW\";");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-app");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
        try{
            consumer.subscribe(List.of("demo"));
            while(true) {
                logger.info("Polling ");
                var records = consumer.poll(Duration.ofMillis(1000));
                for(var record: records) {
                    logger.info("Key :{}value : {}", record.key(), record.value());
                    logger.info("Partition  {}Offset {}", record.partition(), record.offset());
                }

            }
        } catch(WakeupException ex) {
            logger.info("Consumer about to be shutdown");
        } catch(Exception e) {
            logger.error("Error Observer", e);
        } finally {
            consumer.close();
            latch.countDown();
        }
    }
}
