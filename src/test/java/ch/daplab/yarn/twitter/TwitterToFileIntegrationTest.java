package ch.daplab.yarn.twitter;

import ch.daplab.yarn.twill.AbstractTwillLauncher;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@Ignore
public class TwitterToFileIntegrationTest extends AbstractTwillLauncher {

    @Test
    public void test() throws Exception {

        List<String> args = new ArrayList<>();
        args.add("--zk.connect");
        args.add(zkConnect);

        int res = ToolRunner.run(miniCluster.getConfig(), new TwitterToHDFSCli(), args.toArray(new String[0]));

        // wait few more seconds
        Thread.sleep(20000);

    }
}
