package pub.timelyrain.logmining.biz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Counter extends Thread {
    Logger log = LogManager.getLogger(MiningDAO.class);

    private static final AtomicLong count = new AtomicLong(0);
    public static long currentSCN;
    public static long currentSequence;


    public static void addCount(long currentSCN, long currentSequence) {
        Counter.currentSCN = currentSCN;
        Counter.currentSequence = currentSequence;

        count.addAndGet(1);
    }


    @Override
    public void run() {
        long previousCount = 0;
        while (true) {
            log.info("最近10秒处理量为 {}, 总处理量为 {}", count.get() - previousCount, count.get());
            previousCount = count.get();
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                break;
            }
        }

    }
}
