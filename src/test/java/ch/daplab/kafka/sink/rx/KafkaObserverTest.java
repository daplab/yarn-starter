package ch.daplab.kafka.sink.rx;

import ch.daplab.kafka.SetupSimpleKafkaCluster;
import ch.daplab.yarn.twitter.rx.TwitterObservable;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import rx.Observable;
import rx.Observer;
import rx.functions.Func1;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class KafkaObserverTest extends SetupSimpleKafkaCluster {

    private final Random r = new Random();

    @Test
    public void test() throws IOException {

        final int numberOfEvents = r.nextInt(100) + 100;
        final String topic = "TestTopic" + r.nextInt();

        // Create the topic and wait for creation completed
        TestUtils.createTopic(zkClient, topic, 1, 1, JavaConversions.asScalaBuffer(servers), new Properties());
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);

        KafkaObserver kafkaObserver = Mockito.spy(new KafkaObserver(topic, kafkaServersToListOfString(servers)));

        Observable.range(0, numberOfEvents).map(new Func1<Integer, byte[]>() {
            @Override
            public byte[] call(Integer integer) {

                if (integer == null) {
                    return new byte[0];
                } else {
                    byte[] bytes = ByteBuffer.allocate(4).putInt(integer).array();
                    return bytes;
                }
            }
        }).subscribe(kafkaObserver);

        Mockito.verify(kafkaObserver, Mockito.times(numberOfEvents)).onNext(Mockito.<byte[]>any());
        Mockito.verify(kafkaObserver, Mockito.times(1)).onCompleted();
        Mockito.verify(kafkaObserver, Mockito.never()).onError(Mockito.<Throwable>any());


        // Read the messages
        Properties consumerProperties = TestUtils.createConsumerProperties(zkConnect, "groupId", "consumerId", DEFAULT_TIMEOUT);
        ConsumerConfig consumerConfig = new ConsumerConfig(consumerProperties);
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = Collections.singletonMap(topic, NUMBER_OF_THREADS);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        KafkaStream kafkaStream = streams.get(0);
        ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();

        for (int i = 0; i < numberOfEvents; i++) {
            Assert.assertTrue(it.hasNext());
            Assert.assertNotNull(it.next());
        }

    }
}
