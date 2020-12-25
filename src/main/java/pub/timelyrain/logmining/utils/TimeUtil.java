package pub.timelyrain.logmining.utils;

import java.util.concurrent.TimeUnit;

public class TimeUtil {
    public static void sheep(int sec) {
        try {
            TimeUnit.SECONDS.sleep(sec);
        } catch (Exception e) {
        }
    }
}
