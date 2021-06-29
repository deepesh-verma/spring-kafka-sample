package dev.deepesh.springkafkasample;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(classes = TestApplication.class)
@EmbeddedKafka(
        topics = {
            NormalTopicIntegrationTest.NON_RETRYABLE_TOPIC_1,
            NormalTopicIntegrationTest.NON_RETRYABLE_TOPIC_2
        },
        partitions = 1)
class NormalTopicIntegrationTest {

    static final String NON_RETRYABLE_TOPIC_1 = "non-retryable-topic-1";
    static final String NON_RETRYABLE_TOPIC_2 = "non-retryable-topic-2";

    private static final String MAIN_TOPIC_CONTAINER_FACTORY = "kafkaListenerContainerFactory";
    private static final String CUSTOM_TRACE_ID = "custom-trace-id";
    public static final String TRACE_ID = "traceId";

    String firstTopicTraceId;
    String secondTopicTraceId;

    private final CountDownLatch nonRetryableTopicLatch = new CountDownLatch(1);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void whenMessageSendWithB3HeaderToNormalTopic_sameHeaderShouldBeCarriedForward() throws InterruptedException {

        // given
        final ProducerRecord<String, String> record = new ProducerRecord<>(NON_RETRYABLE_TOPIC_1, "Test message");
        record.headers().add(CUSTOM_TRACE_ID, "12345".getBytes());

        // when
        log.info("Sending message :{}", record);
        kafkaTemplate.send(record);
        nonRetryableTopicLatch.await(60, TimeUnit.SECONDS);

        // then
        assertThat(nonRetryableTopicLatch.getCount()).isZero();
        log.info("First topic traceId: {}, Second topic traceId: {}", firstTopicTraceId, secondTopicTraceId);
        assertThat(secondTopicTraceId).isEqualTo(firstTopicTraceId);
    }

    @KafkaListener(id = "test-consumer-1", topics = NON_RETRYABLE_TOPIC_1, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
    public void listenTopic1(ConsumerRecord consumerRecord) {

        log.info("Received: {}", consumerRecord);
        firstTopicTraceId = MDC.get(TRACE_ID);
        kafkaTemplate.send(NON_RETRYABLE_TOPIC_2, (String) consumerRecord.value());
    }

    @KafkaListener(id = "test-consumer-2", topics = NON_RETRYABLE_TOPIC_2, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
    public void listenTopic2(ConsumerRecord consumerRecord) {

        log.info("Received: {}", consumerRecord);
        secondTopicTraceId = MDC.get(TRACE_ID);
        nonRetryableTopicLatch.countDown();
    }

}
