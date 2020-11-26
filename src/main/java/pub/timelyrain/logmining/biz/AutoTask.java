package pub.timelyrain.logmining.biz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import pub.timelyrain.logmining.config.Env;

@Component
public class AutoTask implements ApplicationRunner {
    private final Logger log = LogManager.getLogger(AutoTask.class);
    private final MiningService miningService;
    private final Env env;


    @Autowired
    public AutoTask(MiningService miningService, Env env) {
        this.miningService = miningService;
        this.env = env;
    }




    @Override
    public void run(ApplicationArguments args) {
        log.info(env.toString());
        miningService.startMining();
    }


}
