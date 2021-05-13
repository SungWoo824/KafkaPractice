package com.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class ConsumerRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRebalanceListener.class);
    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);

        Consumer consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());
    }

    private static class RebalanceListener implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener{
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are assigned");
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are revoked");
            consumer.commitSync(currentOffsets);
        }
    }
}
