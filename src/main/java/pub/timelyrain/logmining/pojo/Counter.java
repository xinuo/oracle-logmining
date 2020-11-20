package pub.timelyrain.logmining.pojo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class Counter {
    private Date timestamp;
    private String time;
    private AtomicLong counter;

    public Counter() {
        timestamp = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        time = sdf.format(timestamp);
        counter = new AtomicLong(0);
    }

    public void addCount() {
        counter.addAndGet(1);
    }

    public void addCount(long c) {
        counter.addAndGet(c);
    }
}
