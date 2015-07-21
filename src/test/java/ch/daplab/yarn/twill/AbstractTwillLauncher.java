package ch.daplab.yarn.twill;

import ch.daplab.kafka.SetupSimpleKafkaCluster;
import com.google.common.base.Preconditions;
import kafka.utils.TestZKUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public abstract class AbstractTwillLauncher extends SetupSimpleKafkaCluster {

    protected MiniYARNCluster miniCluster;

    @Before
    public void setup() throws Exception {

        File binJava = new File("/bin/java");
        if (!binJava.isFile()) {
            Preconditions.checkNotNull(System.getenv("JAVA_HOME"), "YARN is using {{JAVA_HOME}}/bin/java to launch the java ApplicationMaster and child processes.\n" +
                    "Please ensure you have JAVA_HOME environment variable set properly (or less recommeneded a symlink to /bin/java)");
        }

        // setup Zookeeper and Kafka in the parent class
        super.setup();

        YarnConfiguration clusterConf = new YarnConfiguration();

        // Keep log files locally for 10 minutes -- mandatory for debugging!!
        clusterConf.set("yarn.nodemanager.delete.debug-delay-sec", String.valueOf(TimeUnit.MINUTES.toSeconds(10)));

        // Set the ZK address into yarn conf. Not required for MiniYARNCluster, but some applications might
        // leverage on this option in real deployment scenarios.
        clusterConf.set("yarn.resourcemanager.zk-address", zkConnect);

        //conf.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
        //conf.setClass(YarnConfiguration.RM_SCHEDULER,
        //        FifoScheduler.class, ResourceScheduler.class);
        miniCluster = new MiniYARNCluster("miniyarn1", 1, 1, 1);
        miniCluster.init(clusterConf);
        miniCluster.start();

        assertTrue("NodeManager(s) failed to connect", miniCluster.waitForNodeManagersToConnect(60000));

    }


    @After
    public void tearDown() throws IOException {
        if (miniCluster != null) {
            miniCluster.stop();
        }

        super.tearDown();
    }
}
