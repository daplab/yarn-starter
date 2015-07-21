package ch.daplab.yarn.twitter;

import ch.daplab.kafka.SetupSimpleKafkaCluster;
import ch.daplab.yarn.twill.AbstractTwillLauncher;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import org.apache.hadoop.conf.Configuration;
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
public class TwitterToKafkaIntegrationTest extends SetupSimpleKafkaCluster {

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

        int res = ToolRunner.run(new Configuration(), new TwitterToKafkaCli(), args.toArray(new String[0]));

        // wait few more seconds
        Thread.sleep(20000);

    }
}
