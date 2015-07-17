package ch.daplab.yarn.twitter;

import ch.daplab.kafka.sink.rx.KafkaObserver;
import ch.daplab.yarn.twitter.rx.TwitterObservable;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class TwitterToKafkaTwillApp extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToKafkaTwillApp.class);

    /**
     * Called by YARN nodeManager, i.e. remote (not on the same JVM) from the command line
     * which starts it.
     */
    @Override
    public void run() {

        final OptionParser parser = new OptionParser();
        parser.allowsUnrecognizedOptions();
        final OptionSet optionSet = parser.parse(getContext().getApplicationArguments());

        final Configuration conf = new Configuration();

        String brokerList = (String) optionSet.valueOf(TwitterToKafkaCli.OPTION_BROKER_LIST);
        String topicName = (String) optionSet.valueOf(TwitterToKafkaCli.OPTION_TOPIC_NAME);

        try {
            Observable.create(new TwitterObservable()).subscribe(new KafkaObserver(topicName, brokerList));
        } finally {
            // noop
        }
    }
}
