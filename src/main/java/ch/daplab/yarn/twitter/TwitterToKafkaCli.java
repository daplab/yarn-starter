package ch.daplab.yarn.twitter;

import ch.daplab.config.Config;
import ch.daplab.kafka.sink.rx.KafkaObserver;
import ch.daplab.yarn.AbstractAppLauncher;
import ch.daplab.yarn.twitter.rx.TwitterObservable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

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

        String brokerList = (String)getOptions().valueOf(OPTION_BROKER_LIST);
        String topicName = (String)getOptions().valueOf(OPTION_TOPIC_NAME);

        Config config = Config.load(TwitterObservable.CONFIG_FILE);

        Observable.create(new TwitterObservable(config.getProperty("oAuthConsumerKey"), config.getProperty("oAuthConsumerSecret"), config.getProperty("oAuthAccessToken"), config.getProperty("oAuthAccessTokenSecret"))).subscribe(new KafkaObserver(topicName, brokerList));

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
