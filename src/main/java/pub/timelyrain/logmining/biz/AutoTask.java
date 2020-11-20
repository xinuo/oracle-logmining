package pub.timelyrain.logmining.biz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import pub.timelyrain.logmining.config.Env;

@Component
public class AutoTask implements ApplicationRunner {
    private final Logger log = LogManager.getLogger(AutoTask.class);
    private final MiningService miningDAO;
    private final Env env;
    private final JdbcTemplate jdbcTemplate;

    public AutoTask(MiningService miningDAO, Env env, JdbcTemplate jdbcTemplate) {
        this.miningDAO = miningDAO;
        this.env = env;
        this.jdbcTemplate = jdbcTemplate;
    }

    @Autowired


    @Override
    public void run(ApplicationArguments args) throws Exception {
//        new CounterService().start();
        log.info(env.toString());
        miningDAO.startMining();
    }


}
