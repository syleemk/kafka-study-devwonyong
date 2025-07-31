package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.util.Properties;

import static org.slf4j.LoggerFactory.getLogger;

public class ProducerWithSyncCallback {
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static Logger logger = getLogger(ProducerWithSyncCallback.class);


    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // acks=0 이면 메타데이터를 가져올 수 없음. test-0@-1 (-1 이라는 오프셋은 없음. 알 수 없다는 뜻)
        // 적재 요청한 리더 파티션 번호는 알 수 있으나 오프셋은 적재 후 에 알 수 있는 값이기 때문에 알 수 없음
        configs.put(ProducerConfig.ACKS_CONFIG, "0");

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "sooyoung", "handsome");
        try {
            // get() method 를 통해서 동기적으로 record 의 메타데이터를 가져올 수 있음
            RecordMetadata metadata = producer.send(record).get();
            // [main] INFO org.example.producer.ProducerWithSyncCallback - test-0@2 (토픽-파티션번호@오프셋)
            // 어떻게 알 수 있나? -> 기본 옵션인 acks=1 이기 때문에 leader 의 acks 통해서 메타데이터를 가져올 수 있음
            logger.info(metadata.toString());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            producer.flush();
            producer.close();
        }
    }
}