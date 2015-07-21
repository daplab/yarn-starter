package ch.daplab.kafka;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public abstract class SetupSimpleKafkaCluster {

    protected static final long DEFAULT_TIMEOUT = 6000;
    protected static final int NUMBER_OF_THREADS = 1;

    private int brokerId = 0;
    protected int zkConnectionTimeout = (int)DEFAULT_TIMEOUT;
    protected int zkSessionTimeout = (int)DEFAULT_TIMEOUT;

    protected String zkConnect;
    protected TestingServer zkServer;
    protected TestingServer zkServer2;
    protected ZkClient zkClient;
    protected KafkaServer kafkaServer;
    protected List<KafkaServer> servers = new ArrayList<>();

    protected CuratorFramework curatorFramework;

    @Before
    public void setup() throws Exception {

        // setup Zookeeper
        zkServer = new TestingServer();
        zkConnect = zkServer.getConnectString();

        curatorFramework = CuratorFrameworkFactory.builder().connectString(zkConnect)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .connectionTimeoutMs(zkConnectionTimeout).sessionTimeoutMs(zkSessionTimeout)
                .build();
        curatorFramework.start();
        assertTrue("Failed to connect to Zookeeper " + zkConnect, curatorFramework.blockUntilConnected(60, TimeUnit.SECONDS));

        zkClient = new ZkClient(zkConnect, zkConnectionTimeout, zkSessionTimeout, ZKStringSerializer$.MODULE$);

        // setup Broker
        int port = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(brokerId, port, true);
        props.put("zookeeper.connect", zkConnect);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        servers.add(kafkaServer);
    }

    @After
    public void tearDown() throws IOException {

        if (curatorFramework != null) {
            curatorFramework.close();
        }

        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }

        if (zkClient != null) {
            zkClient.close();
        }

        if (zkServer != null) {
            zkServer.close();
        }

    }

    protected static String kafkaServersToListOfString(List<KafkaServer> servers) {
        return Joiner.on(",").join(Iterables.transform(servers, new Function<KafkaServer, String>() {
            @Nullable
            @Override
            public String apply(KafkaServer input) {
                return input.socketServer().host() + ":" + input.socketServer().port();
            }
        }));
    }

}
