package cn.lqso.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafka 生产者
 * @author luojie
 * @date 2019-1-7
 */
public class ProducerApi {
    public static void main(String[] args) {
        // 1、配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "bd112:9092");
        props.put("acks", "all");
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2、创建实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 3、发送消息
        int recordSize = 10000;
        for (int i = 0; i < recordSize; i++) {
            /*producer.send(new ProducerRecord<>("chedan", "hello " + i));*/
            // 回调
            producer.send(new ProducerRecord<>("chedan", "hello " + i),
                    (metadata, exception) -> {
                if (metadata != null) {
                    long offset = metadata.offset();
                    int partition = metadata.partition();
                    String topic = metadata.topic();
                    System.out.println(String.format("offset:%d, partition:%d, topic:%s", offset, partition, topic));
                }
            });
            if( i % 100 == 0){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // 4、释放资源
        producer.close();
    }
}
