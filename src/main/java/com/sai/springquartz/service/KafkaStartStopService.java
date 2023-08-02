package com.sai.springquartz.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class KafkaStartStopService {

    public static final long EXECUTION_TIME = 5000L;

    private Logger logger = LoggerFactory.getLogger(getClass());

    private AtomicInteger count = new AtomicInteger();


    @Autowired
    ApplicationContext context;

    public void startJob() {

        logger.info("The Kafka Listener starting...");
        try {
            KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry = context.getBean(KafkaListenerEndpointRegistry.class);
            kafkaListenerEndpointRegistry.start();
            Thread.sleep(EXECUTION_TIME);
        } catch (InterruptedException e) {
            logger.error("Error while executing start job", e);
        } finally {
            count.incrementAndGet();
            logger.info("The Kafka Listener started...");
        }
    }


    public void stopJob() {

        logger.info("The Kafka Listener stopping...");
        try {
            KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry = context.getBean(KafkaListenerEndpointRegistry.class);
            kafkaListenerEndpointRegistry.stop();
            Thread.sleep(EXECUTION_TIME);
        } catch (InterruptedException e) {
            logger.error("Error while executing stop job", e);
        } finally {
            count.incrementAndGet();
            logger.info("The Kafka Listener stopped...");
        }
    }

}
