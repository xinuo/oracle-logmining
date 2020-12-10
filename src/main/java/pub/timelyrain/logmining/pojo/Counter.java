package pub.timelyrain.logmining.pojo;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

public class Counter implements Serializable {
    private static final long serialVersionUID = 1223132132142L;
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

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public AtomicLong getCounter() {
        return counter;
    }

    public void setCounter(AtomicLong counter) {
        this.counter = counter;
    }
}
