package ch.daplab.yarn.twitter;

import ch.daplab.yarn.twill.AbstractTwillLauncher;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import scala.collection.JavaConversions;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Ignore
public class TwitterToKafkaIntegrationTest extends AbstractTwillLauncher {

    private int brokerId = 0;
    protected KafkaServer kafkaServer;
    protected List<KafkaServer> servers = new ArrayList<>();

    @Override
    @Before
    public void setup() throws YarnException, InterruptedException {
        super.setup();

        int port = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(brokerId, port, true);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        servers.add(kafkaServer);
    }

    @After
    public void tearDown() {

        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }
        super.tearDown();
    }

    @Test
    public void test() throws Exception {

        final String topicName = "RandomTopicName123";

        // Create the topic and wait for creation completed
        TestUtils.createTopic(zkClient, topicName, 1, 1, JavaConversions.asScalaBuffer(servers), new Properties());
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topicName, 0, 5000);

        List<String> args = new ArrayList<>();
        args.add("--" + TwitterToKafkaCli.OPTION_ZK_CONNECT);
        args.add(zkConnect);

        args.add("--" + TwitterToKafkaCli.OPTION_BROKER_LIST);
        args.add(kafkaServersToListOfString(servers));

        args.add("--" + TwitterToKafkaCli.OPTION_TOPIC_NAME);
        args.add(topicName);


        int res = ToolRunner.run(miniCluster.getConfig(), new TwitterToKafkaCli(), args.toArray(new String[0]));

        // wait few more seconds
        Thread.sleep(20000);

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
