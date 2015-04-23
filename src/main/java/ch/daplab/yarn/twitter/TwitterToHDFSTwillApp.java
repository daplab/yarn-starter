package ch.daplab.yarn.twitter;

import ch.daplab.fs.sink.PartitionedObserver;
import ch.daplab.yarn.twitter.rx.TwitterObservable;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.twill.api.AbstractTwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;

import static ch.daplab.yarn.twitter.TwitterToHDFSCli.*;

/**
 * Created by mil2048 on 4/22/15.
 */
public class TwitterToHDFSTwillApp extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToHDFSTwillApp.class);

    /**
     * Called by YARN nodeManager, i.e. remote (not on the same JVM) from the command line
     * which startsd it.
     */
    @Override
    public void run() {

        final OptionParser parser = new OptionParser();
        parser.allowsUnrecognizedOptions();
        final OptionSet optionSet = parser.parse(getContext().getApplicationArguments());

        final Configuration conf = new Configuration();

        String defaultFs = (String)optionSet.valueOf(OPTION_FS_DEFAULTFS);

        if (defaultFs != null) {
            conf.set("fs.defaultFS", defaultFs);
        }

        FileSystem fs = null;
        try {
            fs = FileSystem.get(FileSystem.getDefaultUri(conf), conf);

            Observable.create(new TwitterObservable()).subscribe(new PartitionedObserver(DEFAULT_ROOT_FOLDER, DEFAULT_PARTITION_FORMAT, DEFAULT_FILE_SUFFIX, fs));

        } catch (IOException e) {
            LOG.error("Got an IOException", e);
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {}
            }
        }
    }
}
