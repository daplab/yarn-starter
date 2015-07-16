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

    public static final String OPTION_FS_DEFAULTFS = "fs.defaultFS";

    public static final String DEFAULT_ROOT_FOLDER = "/tmp/twitter/firehose/";
    public static final String DEFAULT_FILE_SUFFIX = ".json";
    public static final String DEFAULT_PARTITION_FORMAT = "yyyy/MM/dd/HH/mm";

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

        String defaultFS = (String) getOptions().valueOf(OPTION_FS_DEFAULTFS);
        if (defaultFS == null) {
            defaultFS = FileSystem.getDefaultUri(getConf()).toString();
        }

        // Instantiate TwillRunnerService, and waiting for it to start
        YarnConfiguration yarnConf = new YarnConfiguration(getConf());

        TwillRunnerService runnerService = new YarnTwillRunnerService(
                yarnConf, getZkConnect());

        runnerService.startAndWait();


        List<String> args = new ArrayList<>();
        args.add("--" + OPTION_FS_DEFAULTFS);
        args.add(defaultFS);

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
        getParser().accepts(OPTION_FS_DEFAULTFS, "FileSystem on which to store to. Defaults to fs.defaultFS from /etc/hadoop/conf/core-site.xml.")
                .withRequiredArg();

    }

}
