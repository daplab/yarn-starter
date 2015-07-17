package ch.daplab.fs.sink;

import ch.daplab.fs.sink.partition.Partitioner;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

@NotThreadSafe
public class PartitionedObserver implements Observer<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionedObserver.class);
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private final AtomicBoolean closeRef = new AtomicBoolean(false);

    private final FileSystem fs;
    private final Partitioner partitioner;
    private final boolean appendNewLine = true;

    private FSDataOutputStream currentDataOutputStream;
    private String previousPartition;

    public PartitionedObserver(String prefix, String partitionFormat, String suffix, FileSystem fs) {
        this(prefix, partitionFormat, UTC, suffix, fs);
    }

    public PartitionedObserver(String prefix, String partitionFormat, TimeZone timeZone, String suffix, FileSystem fs) {
        this.fs = fs;
        partitioner = new Partitioner(prefix, partitionFormat, timeZone, suffix);
        LOG.debug("Writing to filesystem {}, partition {}", fs, partitioner);
    }

    @Override
    public void onCompleted() {
        internalClose();
    }

    @Override
    public void onError(Throwable throwable) {
        LOG.warn("Got an exception from the Observable.", throwable);
        internalClose();
    }

    @Override
    public void onNext(byte[] buffer) {

        if (closeRef.get()) {
            return;
        }

        Date d = new Date(System.currentTimeMillis());

        boolean written = false;
        int retry = 0;

        do {
            try {
                final FSDataOutputStream os = getDataOutputStream(d);
                os.write(buffer);
                if (appendNewLine) {
                    os.write('\n');
                }
                written = true;
            } catch (IOException e) {
                LOG.warn("Got an exception while trying to write to " + previousPartition + ". Retrying", e);
                retry++;
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e1) {
                }
            }
        } while (!written && retry < 3);

        if (!written) {
            LOG.error("Not able to write to " + previousPartition + " after 3 retries. Aborting...");
            closeOutputStream();
        }
    }

    private FSDataOutputStream getDataOutputStream(Date date) throws IOException {

        final String partition = partitioner.getPartition(date);
        final String tmpPreviousPartiton = previousPartition;

        if (tmpPreviousPartiton == null || !tmpPreviousPartiton.equals(partition)) {

            closeOutputStream();

            currentDataOutputStream = fs.create(new Path(partition));
            previousPartition = partition;
        }

        return currentDataOutputStream;
    }

    private void internalClose() {
        if (closeRef.compareAndSet(false, true)) {
            closeOutputStream();
        }
    }

    protected void closeOutputStream() {
        final FSDataOutputStream tmpDataOutputStream = currentDataOutputStream;
        if (tmpDataOutputStream != null) {
            currentDataOutputStream = null;
            try {
                tmpDataOutputStream.close();
            } catch (IOException e) {
                LOG.warn("Got an exception while trying to close the current file, filename is " + previousPartition, e);
            }
        }
    }
}
