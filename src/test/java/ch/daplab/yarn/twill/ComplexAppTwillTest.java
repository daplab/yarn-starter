package ch.daplab.yarn.twill;

import ch.daplab.yarn.twill.worker.Consumer;
import ch.daplab.yarn.twill.worker.Producer;
import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.*;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.junit.Test;

import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * "Simple" Twill application which is launching two types of Runnable:
 * <p/>
 * * {@link Producer}: will produce random messages and push them in a Redis queue.
 * Only one instance of the {@link Producer} will be started
 * * {@link Consumer}: will use Twill service discovery to get the Redis instance,
 * and will poll the message from there. Initially 3 instances
 * of {@link Consumer} will be started, and this number might change
 */
public class ComplexAppTwillTest extends AbstractTwillLauncher {

    public static final String REDIS_SERVICE_NAME = "redis1";
    public static final String REDIS_QUEUE_NAME = "queue1";

    @Test(timeout = 120000)
    public void twillSimpleTest() throws InterruptedException, URISyntaxException {

        // Instantiate TwillRunnerService, and waiting for it to start
        YarnConfiguration yarnConf = new YarnConfiguration(miniCluster.getConfig());

        TwillRunnerService runnerService = new YarnTwillRunnerService(
                yarnConf, yarnConf.get("yarn.resourcemanager.zk-address"));

        runnerService.startAndWait();


        // Defining the application.
        // NodeManagers are launching YARN children via a plain
        // java -cp ... MainClass some arguments.
        // These arguments are defined here and will be parsed by
        // apache common cli, jopts or equivalent in the process
        List<String> childrenArgs = new ArrayList<>();
        childrenArgs.add("--mandator.arg");
        childrenArgs.add("important value");


        TwillController controller = runnerService.prepare(new MyTwillApp())
                .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out))) // write TwillRunnable logs in local System.out
                .withResources(getClass().getClassLoader().getResource("log4j.properties").toURI())
                .withApplicationArguments(childrenArgs)
                .enableDebugging()
                .start();

        // Wait until the app is started
        controller.startAndWait();

        // at that point, we can start interacting with the application
        // either checking the state, scaling up and down the number of processes
        // and sending commands to them

        // Just wait until the app completes. A matter of seconds...
        Preconditions.checkState(controller.isRunning());

        // Give some time to the app to complete
        Thread.sleep(20000);

        // Shutting down properly
        controller.stopAndWait();

    }


    /**
     * TwillApplication definition.
     * <p/>
     * This is required only when more than one several different Runnable
     * is started. Otherwise {@link TwillRunnerService#prepare(TwillRunnable)}
     * works perfectly too.
     * <p/>
     * This {@link TwillApplication} will start two {@link Runnable}:
     * * 1 instance of {@link Producer}
     * * 3 instances of {@link Consumer}
     */
    public static class MyTwillApp implements TwillApplication {

        @Override
        public TwillSpecification configure() {
            //try {
            return TwillSpecification.Builder.with()
                    .setName(MyTwillApp.class.getName())

                    .withRunnable()

                    .add(Producer.class.getName(), new Producer(), ResourceSpecification.Builder.with()
                            .setVirtualCores(1)
                            .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
                            .setInstances(1)
                            .build()).noLocalFiles()
                            //withLocalFiles()
                            //.add("log4j.properties", getClass().getClassLoader().getResource("log4j.properties").toURI()).apply()

                    .add(Consumer.class.getName(), new Consumer(), ResourceSpecification.Builder.with()
                            .setVirtualCores(1)
                            .setMemory(256, ResourceSpecification.SizeUnit.MEGA)
                            .setInstances(2)
                            .build()).noLocalFiles()

                            //.withOrder()
                            //.begin(Producer.class.getName())
                            //.nextWhenStarted(Consumer.class.getName())
                    .anyOrder()
                    .build();
            //} catch (URISyntaxException e) {
            //    e.printStackTrace();
            //    throw new RuntimeException("Can't find classpath:log4j.properties", e);
            //}
        }
    }
}
