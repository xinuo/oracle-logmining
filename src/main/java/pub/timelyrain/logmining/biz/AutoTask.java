package pub.timelyrain.logmining.biz;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import pub.timelyrain.logmining.sys.ShutdownHook;

@Component
public class AutoTask implements ApplicationRunner {
    MiningDAO miningDAO;

    @Autowired
    public AutoTask(MiningDAO miningDAO) {
        this.miningDAO = miningDAO;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());

        miningDAO.startMining();
    }
}
