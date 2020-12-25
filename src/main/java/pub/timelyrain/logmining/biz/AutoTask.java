package pub.timelyrain.logmining.biz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import pub.timelyrain.logmining.config.Env;

@Component
public class AutoTask implements ApplicationRunner {
    private final Logger log = LogManager.getLogger(AutoTask.class);
    private final ExtractService miningService;
    private final ReplicateService replicateService;
    private final Env env;
    private final RedisTemplate redisTemplate;

    @Autowired
    public AutoTask(ExtractService miningService, ReplicateService replicateService, Env env, RedisTemplate redisTemplate) {
        this.miningService = miningService;
        this.env = env;
        this.redisTemplate = redisTemplate;
        this.replicateService = replicateService;
    }


    @Override
    public void run(ApplicationArguments args) {
        log.info(env.init());

        miningService.start();
        replicateService.start();

    }


}
