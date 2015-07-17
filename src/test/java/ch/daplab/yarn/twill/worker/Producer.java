package ch.daplab.yarn.twill.worker;

import ch.daplab.yarn.twill.ComplexAppTwillTest;
import com.google.common.base.Preconditions;
import org.apache.twill.api.AbstractTwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;
import redis.embedded.ports.EphemeralPortProvider;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Producer extends AbstractTwillRunnable {

    private static Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private static final String JAVA_IO_TMP_PROPERTY = "java.io.tmpdir";
    private static final String TMP_DIR = "/var/tmp";

    private final Random RANDOM = new Random();

    @Override
    public void run() {

        LOG.info(getClass().getCanonicalName() + " starts");

        // We could parse the applications received by the command line
        LOG.info(Arrays.toString(getContext().getApplicationArguments()));


        // In many servers, executables can't be run from /tmp.
        // Temporarily changing to TMP_DIR to start redis server from there.
        String oldJavaIoTmpDir = System.getProperty(JAVA_IO_TMP_PROPERTY);

        File javaIoTmpdir = new File(TMP_DIR);
        if (javaIoTmpdir.isDirectory()) {
            System.setProperty(JAVA_IO_TMP_PROPERTY, TMP_DIR);
        }


        // Start redis with a random port to avoid conflict of serveral processes trying to use the same port
        RedisServer redisServer = null;

        try {
            try {
                EphemeralPortProvider epp = new EphemeralPortProvider();
                redisServer = new RedisServer(epp.next());
                //# maxmemory <bytes>
                //# bind 127.0.0.1 and data ip only
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
            redisServer.start();

            LOG.info("Emebedded redis server started");

            // reverting the change once redis is started.
            System.setProperty(JAVA_IO_TMP_PROPERTY, oldJavaIoTmpDir);

            List<Integer> ports = redisServer.ports();

            Preconditions.checkState(ports.size() == 1);
            final int redisPort = Integer.valueOf(ports.get(0));


            // Announcing redis port to the other processes
            getContext().announce(ComplexAppTwillTest.REDIS_SERVICE_NAME, redisPort);


            LOG.info("Redis service announced at port " + redisPort);

            // Instantiate a local redis client to generate some data
            Jedis jedis = null;

            try {
                jedis = new Jedis(InetAddress.getLocalHost().getHostName(), redisPort);

            } catch (UnknownHostException e) {
                LOG.error("Don't know this hostname", e);
                throw new RuntimeException(e);
            }

            // Produce [500, 1000[ messages
            int numberOfMessages = RANDOM.nextInt(500) + 500;

            for (int i = 0; i < numberOfMessages; i++) {
                jedis.rpush(ComplexAppTwillTest.REDIS_QUEUE_NAME, "awesome-msg-" + String.format("%3d", i));
            }

            LOG.info("Published " + numberOfMessages + " messages into redis");

            // closing the client and in turn the runnable.
            jedis.close();

            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(60));
            } catch (InterruptedException e) {
                // let this process silently die.
            }

            LOG.info("Now closing.");

        } finally {
            if (redisServer != null) {
                redisServer.stop();
            }
        }
    }
}
