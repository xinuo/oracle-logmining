package pub.timelyrain.logmining.biz;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import pub.timelyrain.logmining.pojo.Counter;

import java.util.List;
import java.util.Vector;

@Component
public class CounterService {
    private static final Logger log = LogManager.getLogger(MiningService.class);
    private static final List<Counter> traceList = new Vector();

//    public static long currentSCN;
//    public static long currentSequence;

    public void addCount(long currentSCN, long currentSequence) {
        if (traceList.isEmpty()) {
            Counter counter = new Counter();
            traceList.add(counter);
        }
        traceList.get(traceList.size() - 1).addCount();
    }


    @Scheduled(cron = "* * * * * ? *")
    public void startCounter() {
        if (traceList.size() >= 300)
            traceList.remove(0);

        traceList.add(new Counter());
    }

    public String status() {
        ObjectMapper om = new ObjectMapper();
        om.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            return om.writeValueAsString(traceList);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
    public List<Counter> traceList(){
        return traceList;
    }
}
