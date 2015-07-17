package ch.daplab.yarn.twill.worker;

import ch.daplab.yarn.twill.ComplexAppTwillTest;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Consumer extends AbstractTwillRunnable implements ServiceDiscovered.ChangeListener {

    private static Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicReference<Jedis> jedisClientRef = new AtomicReference<>(null);
    private final CountDownLatch discoveredLatch = new CountDownLatch(1);

    private final AtomicBoolean runRef = new AtomicBoolean(true);

    @Override
    public void run() {

        LOG.info(getClass().getCanonicalName() + " starts");

        try {
            // Discover redis service. The service end point might change over time,
            // so this class is listening to service change and use a reference to the
            // redis client which might be updated asynchronously.
            ServiceDiscovered redisService = getContext().discover(ComplexAppTwillTest.REDIS_SERVICE_NAME);
            redisService.watchChanges(this, executor);

            // Wait until a first service is discovered.
            // We specify in {@link ComplexAppTwillTest.MyTwillApp} an order of start (Producer, then Consumers)
            // but this not required when doing such wait
            try {
                do {

                } while (runRef.get() && !discoveredLatch.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            while (runRef.get()) {

                final Jedis j = jedisClientRef.get();

                // message.get(0) contains the name of the Redis list the message comes from
                // message.get(1) contains the value.
                List<String> message = j.blpop(10, ComplexAppTwillTest.REDIS_QUEUE_NAME);

                if (message == null || message.isEmpty()) {
                    // if we didn't recevie any more messages withing a minute
                    // it's most likely the Producer stopped.
                    // So shutting down this worker too.
                    break;
                }

                // do something meaningful with message.get(1)
                LOG.info("Woot, received a message: " + message.get(1));
            }
        } catch (redis.clients.jedis.exceptions.JedisConnectionException e) {
            LOG.warn("Got an exception while communicating with redis. Might not be a big deal...", e);
        } finally {
            final Jedis j = jedisClientRef.get();
            if (j != null) {
                j.close();
            }
        }

    }

    @Override
    public void onChange(ServiceDiscovered serviceDiscovered) {
        Iterator<Discoverable> it = serviceDiscovered.iterator();
        while (it.hasNext()) {
            Discoverable d = it.next();
            Jedis j = jedisClientRef.get();
            if (j == null ||
                    (!d.getSocketAddress().getHostName().equals(j.getClient().getHost())
                            || j.getClient().getPort() != d.getSocketAddress().getPort())) {

                LOG.info("Discovered redis service at " + d.getSocketAddress().getHostName() + ":" + d.getSocketAddress().getPort());
                j = new Jedis(d.getSocketAddress().getHostName(), d.getSocketAddress().getPort());

                jedisClientRef.set(j);
                discoveredLatch.countDown();
            }
        }
    }
}
