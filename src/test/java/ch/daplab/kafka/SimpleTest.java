package ch.daplab.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import scala.collection.JavaConversions;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Kafka test class: instantiate embedded ZK and Kafka clusters
 * Instantiate consumer group, and start writing
 */
public class SimpleTest extends SetupSimpleKafkaCluster {

    private static final String topic = "test123";

    private static final Random r = new Random();

    @Test
    public void testCreateTopicWriteAndReadWithConsumerGroup() throws InterruptedException {

        // How many message to write as well as stop condition
        final int numberOfMessages = r.nextInt(90) + 10;
        final CountDownLatch counterLatch = new CountDownLatch(numberOfMessages);
        final CountDownLatch consumerInitialized = new CountDownLatch(NUMBER_OF_THREADS);

        // Create the topic and wait for creation completed
        TestUtils.createTopic(zkClient, topic, 1, 1, JavaConversions.asScalaBuffer(servers), new Properties());
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);

        // Instantiate all the plumbing for consuming the messages
        ExecutorService executorService = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

        Properties consumerProperties = TestUtils.createConsumerProperties(zkConnect, "groupId", "consumerId", DEFAULT_TIMEOUT);
        ConsumerConfig consumerConfig = new ConsumerConfig(consumerProperties);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = Collections.singletonMap(topic, NUMBER_OF_THREADS);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            executorService.submit(new ConsumerTest(streams.get(i), i, consumerInitialized, counterLatch));
        }
        // No need to allow more job submission
        executorService.shutdown();

        boolean initialized = consumerInitialized.await(5, TimeUnit.SECONDS);
        Assert.assertTrue(initialized);

        // Setup producer
        Properties producerProperties = TestUtils.getProducerConfig(kafkaServersToListOfString(servers));
        //producerProperties.setProperty("request.required.acks", "1"); // default is -1 which gives stronger guarantees
        // @see https://kafka.apache.org/08/configuration.html

        ProducerConfig producerConfig = new ProducerConfig(producerProperties);
        Producer producer = new Producer(producerConfig);

        // Generate the message to send
        KeyedMessage<Integer, byte[]> data = new KeyedMessage(topic, "test-message".getBytes());

        // And send it
        for (int i = 0; i < numberOfMessages; i++) {
            producer.send(data);
        }

        // Wait until message completed, but in a bounded amount of time
        boolean completed = counterLatch.await(5, TimeUnit.SECONDS);

        Assert.assertTrue(completed);

    }

    public class ConsumerTest implements Runnable {
        private final KafkaStream stream;
        private final int threadNumber;
        private final CountDownLatch initializationLatch;
        private final CountDownLatch counterLatch;

        public ConsumerTest(KafkaStream stream, int threadNumber, CountDownLatch initializationLatch, CountDownLatch counterLatch) {
            this.threadNumber = threadNumber;
            this.stream = stream;
            this.initializationLatch = initializationLatch;
            this.counterLatch = counterLatch;
        }

        public void run() {
            initializationLatch.countDown();
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                System.out.println("Thread " + threadNumber + ": " + new String(it.next().message()));
                counterLatch.countDown();
            }
        }
    }
}
