package ch.daplab.yarn.twitter;

import ch.daplab.fs.sink.PartitionedObserver;
import ch.daplab.yarn.twitter.rx.TwitterObservable;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.twill.api.AbstractTwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.util.Arrays;

import static ch.daplab.yarn.twitter.TwitterToHDFSCli.*;

public class TwitterToHDFSTwillApp extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToHDFSTwillApp.class);

    /**
     * Called by YARN nodeManager, i.e. remote (not on the same JVM) from the command line
     * which starts it.
     */
    @Override
    public void run() {

        String[] arguments = (String[])ArrayUtils.addAll(getContext().getApplicationArguments(), getContext().getArguments());

        final OptionParser parser = new OptionParser();

        TwitterToHDFSCli.initParser(parser);
        parser.accepts("oAuthConsumerKey").withRequiredArg();
        parser.accepts("oAuthConsumerSecret").withRequiredArg();
        parser.accepts("oAuthAccessToken").withRequiredArg();
        parser.accepts("oAuthAccessTokenSecret").withRequiredArg();

        parser.allowsUnrecognizedOptions();
        final OptionSet optionSet = parser.parse(arguments);

        final Configuration conf = new Configuration();

        String defaultFs = (String) optionSet.valueOf(OPTION_FS_DEFAULTFS);

        if (defaultFs != null) {
            conf.set("fs.defaultFS", defaultFs);
        }

        String rootFolder = (String) optionSet.valueOf(OPTION_ROOT_FOLDER);
        String partitionFormat = (String) optionSet.valueOf(OPTION_PARTITION_FORMAT);
        String fileSuffix = (String) optionSet.valueOf(OPTION_FILE_SUFFIX);

        String oAuthConsumerKey = (String) optionSet.valueOf("oAuthConsumerKey");
        String oAuthConsumerSecret = (String) optionSet.valueOf("oAuthConsumerSecret");
        String oAuthAccessToken = (String) optionSet.valueOf("oAuthAccessToken");
        String oAuthAccessTokenSecret = (String) optionSet.valueOf("oAuthAccessTokenSecret");

        FileSystem fs = null;
        try {
            fs = FileSystem.get(FileSystem.getDefaultUri(conf), conf);

            Observable.create(new TwitterObservable(oAuthConsumerKey, oAuthConsumerSecret, oAuthAccessToken, oAuthAccessTokenSecret))
                    .subscribe(new PartitionedObserver(rootFolder, partitionFormat, fileSuffix, fs));

        } catch (IOException e) {
            LOG.error("Got an IOException", e);
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                }
            }
        }
    }
}
