package ch.daplab.yarn.twitter;

import ch.daplab.yarn.AbstractAppLauncher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mil2048 on 4/22/15.
 */
public class TwitterToKafkaCli extends AbstractAppLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaCli.class);

    public static final String OPTION_BROKER_LIST = "broker.list";
    public static final String OPTION_BROKER_LIST_DEFAULT = "daplab-rt-11.fri.lan:6667,daplab-rt-13.fri.lan:6667,daplab-rt-14.fri.lan:6667";

    public static final String OPTION_TOPIC_NAME = "topic.name";
    public static final String OPTION_TOPIC_NAME_DEFAULT = "twitter";

    private Configuration conf;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TwitterToKafkaCli(), args);
        System.exit(res);
    }

    /**
     * Called by {@link org.apache.hadoop.util.ToolRunner#run(org.apache.hadoop.conf.Configuration, org.apache.hadoop.util.Tool, String[])} and in turn {@link ch.daplab.yarn.AbstractAppLauncher#run(String[])},
     * i.e. local to the server where the command line is launched.
     */
    @Override
    protected int internalRun() throws Exception {

        // Instantiate TwillRunnerService, and waiting for it to start
        YarnConfiguration yarnConf = new YarnConfiguration(getConf());

        TwillRunnerService runnerService = new YarnTwillRunnerService(
                yarnConf, getZkConnect());

        runnerService.startAndWait();

        String brokerList = (String)getOptions().valueOf(OPTION_BROKER_LIST);

        String topicName = (String)getOptions().valueOf(OPTION_TOPIC_NAME);

        List<String> args = new ArrayList<>();
        args.add("--" + OPTION_BROKER_LIST);
        args.add(brokerList);
        args.add("--" + OPTION_TOPIC_NAME);
        args.add(topicName);

        TwillController controller = runnerService.prepare(new TwitterToKafkaTwillApp())
                .withApplicationArguments(args.toArray(new String[0]))
                .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out))) // write TwillRunnable logs in local System.out
                .start();

        // Wait until the app is started
        controller.startAndWait();

        // Good, let it simply run on his own.
        return ReturnCode.ALL_GOOD;
    }

    @Override
    protected void initParser() {
        getParser().accepts(OPTION_BROKER_LIST, "Kafka broker list, comma separated")
                .withRequiredArg().defaultsTo(OPTION_BROKER_LIST_DEFAULT);
        getParser().accepts(OPTION_TOPIC_NAME, "Name of the Kafka topic")
                .withRequiredArg().defaultsTo(OPTION_TOPIC_NAME_DEFAULT);
    }

}
