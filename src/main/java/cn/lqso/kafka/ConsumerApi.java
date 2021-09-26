package cn.lqso.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * kafka 消费者
 * @author luojie
 * @date 2019-1-7
 */
public class ConsumerApi {
    public static void main(String[] args) {
        // 1、配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "bd113:9092");
        props.put("group.id", "g1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2、创建实例
        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);
        
        // 3、消费
        consumer.subscribe(Arrays.asList("chedan"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            System.out.println("--------------");
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(), record.key(), record.value());
            }
        }
    }
}