package ch.daplab.fs.sink.partition;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Partitioner {


    private final TimeZone timezone;
    private final String simpleDateFormatString;
    private final SimpleDateFormat simpleDateFormat;
    private final String prefix;
    private final String suffix;

    public Partitioner(String prefix, String simpleDateFormatString, TimeZone timezone, String suffix) {
        this.prefix = prefix;
        this.simpleDateFormatString = simpleDateFormatString;
        this.simpleDateFormat = new SimpleDateFormat(simpleDateFormatString);
        this.simpleDateFormat.setTimeZone(timezone);
        this.timezone = timezone;
        this.suffix = suffix;
    }

    public String getPartition(Date date) {
        final StringBuilder sb = new StringBuilder(prefix);
        sb.append(simpleDateFormat.format(date));
        sb.append(suffix);
        return sb.toString();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getCanonicalName());
        sb.append("[")
                .append(prefix)
                .append("|")
                .append(simpleDateFormatString)
                .append("|")
                .append(suffix);
        return sb.toString();
    }
}
