package dev.deepesh.springkafkasample;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(classes = TestApplication.class)
@EmbeddedKafka(
        topics = {
            RetryableTopicIntegrationTest.MAIN_RETRYABLE_TOPIC
        },
        partitions = 1)
class RetryableTopicIntegrationTest {

    static final String MAIN_RETRYABLE_TOPIC = "main-retryable-topic";

    private static final String MAIN_TOPIC_CONTAINER_FACTORY = "kafkaListenerContainerFactory";
    private static final String CUSTOM_TRACE_ID = "custom-trace-id";
    public static final String TRACE_ID = "traceId";

    String mainTopicTraceId;
    String retryTopicTraceId;

    private final CountDownLatch retryableTopicLatch = new CountDownLatch(1);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void whenMessageSendWithB3HeaderToRetryableTopic_sameHeaderShouldBeCarriedForward() throws InterruptedException {

        // given
        final ProducerRecord<String, String> record = new ProducerRecord<>(MAIN_RETRYABLE_TOPIC, "Test message");
        record.headers().add(CUSTOM_TRACE_ID, "12345".getBytes());

        // when
        log.info("Sending message :{}", record);
        kafkaTemplate.send(record);
        retryableTopicLatch.await(60, TimeUnit.SECONDS);

        // then
        assertThat(retryableTopicLatch.getCount()).isZero();
        log.info("Retry topic traceId: {}, Main topic traceId: {}", retryTopicTraceId, mainTopicTraceId);
        assertThat(retryTopicTraceId).isEqualTo(mainTopicTraceId);
    }

    @RetryableTopic(dltStrategy = DltStrategy.NO_DLT, attempts = "2")
    @KafkaListener(id = "test-consumer", topics = MAIN_RETRYABLE_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
    public void listen(ConsumerRecord consumerRecord) {

        log.info("Received: {}", consumerRecord);
        Headers headers = consumerRecord.headers();

        String traceId = MDC.get(TRACE_ID);
        if (Objects.nonNull(headers.lastHeader(KafkaHeaders.ORIGINAL_TOPIC))) {
            this.retryTopicTraceId = traceId;
            retryableTopicLatch.countDown();
        } else {
            mainTopicTraceId = traceId;
        }

        throw new RuntimeException("Woooops... in topic with consumerRecord: " + consumerRecord);
    }

}
