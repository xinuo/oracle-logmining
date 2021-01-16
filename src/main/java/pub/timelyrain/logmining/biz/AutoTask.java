package pub.timelyrain.logmining.biz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import pub.timelyrain.logmining.config.BeanContextConfig;
import pub.timelyrain.logmining.config.Env;

@Component
public class AutoTask implements ApplicationRunner {
    private final Logger log = LogManager.getLogger(AutoTask.class);
    private final ReplicateService replicateService;
    private final Env env;
    private final RedisTemplate redisTemplate;

    @Autowired
    public AutoTask(ReplicateService replicateService, Env env, RedisTemplate redisTemplate) {

        this.env = env;
        this.redisTemplate = redisTemplate;
        this.replicateService = replicateService;
    }


    @Override
    public void run(ApplicationArguments args) {
        log.info("启动抓取,并初始化表与交换机关系：{}",env.init());

        for(int i=0;i<2;i++){
            ExtractService extractService = BeanContextConfig.getBean(ExtractService.class);
            int i1 = extractService.hashCode();
            log.debug("extractService.hashCode()：{}",i1);
            extractService.setCurrentThread(i+1);
            extractService.start();

        }
        replicateService.start();

    }


}
