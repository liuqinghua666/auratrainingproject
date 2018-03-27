package org.training.spark.graduation2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.training.spark.util.KafkaRedisConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by qinghua.liu on 3/29/18.
 * kafa消息消费客户端，用于测试
 */
public class JavaKafkaEventConsumer {

    //config
    public static Properties getConfig()
    {
        Properties props = new Properties();
        //props.put("bootstrap.servers", "localhost:9092");
        props.put("bootstrap.servers", KafkaRedisConfig.KAFKA_ADDR);
        props.put("group.id", "testGroup");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

    public void consumeMessage()
    {
        // launch 3 threads to consume
        int numConsumers = 1;
        final String topic = KafkaRedisConfig.KAFKA_USER_PAY_TOPIC;
        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        final List<KafkaConsumerRunner> consumers = new ArrayList<KafkaConsumerRunner>();
        for (int i = 0; i < numConsumers; i++) {
            KafkaConsumerRunner consumer = new KafkaConsumerRunner(topic);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                for (KafkaConsumerRunner consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    // Thread to consume kafka data
    public static class KafkaConsumerRunner
            implements Runnable
    {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer<String, String> consumer;
        private final String topic;

        public KafkaConsumerRunner(String topic)
        {
            Properties props = getConfig();
            consumer = new KafkaConsumer<String, String>(props);
            this.topic = topic;
        }

        public void handleRecord(ConsumerRecord record)
        {
            System.out.println(new java.util.Date()+"<== name: " + Thread.currentThread().getName() + " ; topic: " + record.topic() + " ; offset" + record.offset() + " ; key: " + record.key() + " ; value: " + record.value()+ " ; timestamp: " + record.timestamp());
        }

        public void run()
        {
            try {
                // subscribe
                consumer.subscribe(Arrays.asList(topic));
                while (!closed.get()) {
                    //read data
                    ConsumerRecords<String, String> records = consumer.poll(10000);
                    // Handle new records
                    for (ConsumerRecord<String, String> record : records) {
                        handleRecord(record);
                    }
                }
            }
            catch (Exception e) {
                // Ignore exception if closing
                if (!closed.get()) {
                    throw e;
                }
            }
            finally {
                consumer.close();
            }
        }

        // Shutdown hook which can be called from a separate thread
        public void shutdown()
        {
            closed.set(true);
            consumer.wakeup();
        }
    }

    public static void main(String[] args)
    {
        JavaKafkaEventConsumer example = new JavaKafkaEventConsumer();
        example.consumeMessage();
    }

}
