package ch.daplab.fs.sink;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import rx.Observable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static ch.daplab.yarn.twitter.TwitterToHDFSCli.DEFAULT_FILE_SUFFIX;
import static ch.daplab.yarn.twitter.TwitterToHDFSCli.DEFAULT_PARTITION_FORMAT;

public class PartitionObserverTest {

    private Random r = new Random();

    @Test
    public void test() throws IOException {

        FileSystem fs = FileSystem.getLocal(new Configuration());

        Path p = new Path(Files.createTempDir().toURI());

        PartitionedObserver partitionedObserver = new PartitionedObserver(p.toString(), DEFAULT_PARTITION_FORMAT, DEFAULT_FILE_SUFFIX, fs);

        byte[] payload = new byte[128];
        Arrays.fill(payload, "c".getBytes()[0]);

        final int numberOfBytesArrays = r.nextInt(100) + 100;

        List<byte[]> data = new ArrayList<>(numberOfBytesArrays);
        for (int i = 0; i < numberOfBytesArrays; i++) {
            data.add(payload);
        }
        PartitionedObserver spy = Mockito.spy(partitionedObserver);

        Observable.from(data).subscribe(spy);

        Mockito.verify(spy, Mockito.times(numberOfBytesArrays)).onNext(Mockito.<byte[]>any());

        long totalStoredSize = 0L;
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(p, true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus lfs = locatedFileStatusRemoteIterator.next();
            System.out.println(lfs.toString() + ": " + lfs.getLen());
            if (lfs.isFile()) {
                totalStoredSize += lfs.getLen();
            }
        }

        Assert.assertEquals(numberOfBytesArrays * (128 + 1), totalStoredSize); // 128 + 1 because we're adding newline.

    }
}
