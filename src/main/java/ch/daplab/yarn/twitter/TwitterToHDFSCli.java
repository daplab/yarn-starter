package ch.daplab.yarn.twitter;

import ch.daplab.config.Config;
import ch.daplab.yarn.AbstractAppLauncher;
import ch.daplab.yarn.twitter.rx.TwitterObservable;
import joptsimple.OptionParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
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

public class TwitterToHDFSCli extends AbstractAppLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterToHDFSCli.class);

    public static final String OPTION_FS_DEFAULTFS = "fs.defaultFS";
    public static final String OPTION_ROOT_FOLDER = "root.folder";
    public static final String OPTION_PARTITION_FORMAT = "partition.format";
    public static final String OPTION_FILE_SUFFIX = "file.suffix";

    public static final String DEFAULT_ROOT_FOLDER = "/tmp/twitter/firehose/";
    public static final String DEFAULT_FILE_SUFFIX = ".json";
    public static final String DEFAULT_PARTITION_FORMAT = "yyyy/MM/dd/HH/mm";

    private Configuration conf;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TwitterToHDFSCli(), args);
        System.exit(res);
    }

    /**
     * Called by {@link ToolRunner#run(Configuration, Tool, String[])} and in turn {@link AbstractAppLauncher#run(String[])},
     * i.e. local to the server where the command line is launched.
     */
    @Override
    protected int internalRun() throws Exception {

        String defaultFS = (String) getOptions().valueOf(OPTION_FS_DEFAULTFS);
        if (defaultFS == null) {
            defaultFS = FileSystem.getDefaultUri(getConf()).toString();
        }

        String rootFolder = (String) getOptions().valueOf(OPTION_ROOT_FOLDER);
        String partitionFormat = (String) getOptions().valueOf(OPTION_PARTITION_FORMAT);
        String fileSuffix = (String) getOptions().valueOf(OPTION_FILE_SUFFIX);

        // Instantiate TwillRunnerService, and waiting for it to start
        YarnConfiguration yarnConf = new YarnConfiguration(getConf());

        TwillRunnerService runnerService = new YarnTwillRunnerService(
                yarnConf, getZkConnect());

        runnerService.startAndWait();

        List<String> args = new ArrayList<>();
        args.add("--" + OPTION_FS_DEFAULTFS);
        args.add(defaultFS);

        args.add("--" + OPTION_ROOT_FOLDER);
        args.add(rootFolder);

        args.add("--" + OPTION_PARTITION_FORMAT);
        args.add(partitionFormat);

        args.add("--" + OPTION_FILE_SUFFIX);
        args.add(fileSuffix);

        Config config = Config.load(TwitterObservable.CONFIG_FILE);
        args.add("--oAuthConsumerKey");
        args.add(config.getProperty("oAuthConsumerKey"));
        args.add("--oAuthConsumerSecret");
        args.add(config.getProperty("oAuthConsumerSecret"));
        args.add("--oAuthAccessToken");
        args.add(config.getProperty("oAuthAccessToken"));
        args.add("--oAuthAccessTokenSecret");
        args.add(config.getProperty("oAuthAccessTokenSecret"));

        TwillController controller = runnerService.prepare(new TwitterToHDFSTwillApp())
                .withApplicationArguments(args.toArray(new String[0]))
                .withResources(getClass().getResource(TwitterObservable.CONFIG_FILE).toURI())
                .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out))) // write TwillRunnable logs in local System.out
                .start();

        // Wait until the app is started
        controller.startAndWait();

        // Good, let it simply run on his own.
        return ReturnCode.ALL_GOOD;
    }

    @Override
    protected void initParser() {
        initParser(getParser());
    }

    static void initParser(OptionParser parser) {
        parser.accepts(OPTION_FS_DEFAULTFS, "FileSystem on which to store to. Defaults to fs.defaultFS from /etc/hadoop/conf/core-site.xml.")
                .withRequiredArg();
        parser.accepts(OPTION_ROOT_FOLDER, "The root folder, inside the fileSystem, where the data is stored")
                .withRequiredArg().defaultsTo(DEFAULT_ROOT_FOLDER);
        parser.accepts(OPTION_PARTITION_FORMAT, "The partitioning format, inside the root folder -- accepts Joda time formatting")
                .withRequiredArg().defaultsTo(DEFAULT_PARTITION_FORMAT);
        parser.accepts(OPTION_FILE_SUFFIX, "The suffix of the file name")
                .withRequiredArg().defaultsTo(DEFAULT_FILE_SUFFIX);
    }
}
