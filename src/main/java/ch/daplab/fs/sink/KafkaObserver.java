package ch.daplab.fs.sink;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by mil2048 on 4/22/15.
 */
@NotThreadSafe
public class KafkaObserver implements Observer<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaObserver.class);
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private final AtomicBoolean closeRef = new AtomicBoolean(false);

    private final String topic;
    private final Producer producer;

    public KafkaObserver(String topic) {
        this.topic = topic;

        Properties props = new Properties();

        props.put("metadata.broker.list", "daplab-rt-11.fri.lan:6667,daplab-rt-13.fri.lan:6667");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig producerConfig = new ProducerConfig(props);

        producer = new Producer(producerConfig);
    }

    @Override
    public void onCompleted() {
        internalClose();
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.warn("Got an exception from the Observable.", throwable);
        internalClose();
    }

    @Override
    public void onNext(byte[] buffer) {

        if (closeRef.get()) {
            return;
        }

        KeyedMessage<Integer, byte[]> data = new KeyedMessage<>(topic, buffer);
        producer.send(data);

    }

    private void internalClose() {
        if (closeRef.compareAndSet(false, true)) {
            if (producer != null) {
                producer.close();
            }
        }
    }

}
