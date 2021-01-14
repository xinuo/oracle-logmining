package pub.timelyrain.logmining.biz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import pub.timelyrain.logmining.config.Env;
import pub.timelyrain.logmining.pojo.RedoLog;
import pub.timelyrain.logmining.pojo.Row;
import pub.timelyrain.logmining.utils.TimeUtil;

@Component
@Scope("prototype")
public class ReplicateService extends Thread {
    private static final Logger log = LogManager.getLogger(ReplicateService.class);
    private RedisTemplate redisTemplate;
    private RabbitTemplate rabbitTemplate;
    private SQLExtractor sqlExtractor;
    private Env env;

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            try {
                long replicateQueueCount = redisTemplate.opsForList().size(Constants.QUEUE_REPLICATE);
                if (replicateQueueCount == 0) {
                    TimeUtil.sheep(1);
                    continue;
                }
                String xid = (String) redisTemplate.opsForList().index(Constants.QUEUE_REPLICATE, 0);
                replicate(xid);
                redisTemplate.opsForList().leftPop(Constants.QUEUE_REPLICATE);
            } catch (Exception e) {
                log.error("读取待同步队列错误", e);
            }
        }
    }

    private void replicate(String xid) {
        String txKey = "mining:xid:" + xid;
        //从redis中取得redolog的列表，并调用convertAndDelivery
        while (redisTemplate.opsForList().size(txKey) != 0 && !Thread.interrupted()) {
            try {
                RedoLog redo = (RedoLog) redisTemplate.opsForList().index(txKey, 0);
                convertAndDelivery(redo);
                redisTemplate.opsForList().leftPop(txKey);
            } catch (Exception e) {
                log.error("复制数据至MQ错误", e);
            }
        }
        log.info("事务 {} 数据推送完毕", xid);
        redisTemplate.delete(txKey);

    }

    private void convertAndDelivery(RedoLog redoLog) {
        try {
            Row row = sqlExtractor.parse(redoLog.getSchema(), redoLog.getTableName(), redoLog.getRedo());
            if (row == null)
                return;

            row.setRowId(redoLog.getRowId());
            row.setScn(redoLog.getScn());
            row.setCommitScn(redoLog.getCommitScn());
            row.setTimestamp(redoLog.getTimestamp());
            rabbitTemplate.convertAndSend(env.getExchangeName(row.getSchemaName(), row.getTableName()), redoLog.getSchema() + "." + redoLog.getTableName(), row);

            log.debug("发送数据,类型{}.{} 表为{}.{}, sql is {}", row.getMode(), row.getOperator(), row.getSchemaName(), row.getTableName(), row.getSql());
        } catch (Exception e) {
            log.error("转换REDO和分发数据错误 scn:{}", redoLog.toString(),e);
        }
    }

    @Autowired
    public void setRedisTemplate(RedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Autowired
    public void setRabbitTemplate(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @Autowired
    public void setSqlExtractor(SQLExtractor sqlExtractor) {
        this.sqlExtractor = sqlExtractor;
    }

    @Autowired
    public void setEnv(Env env) {
        this.env = env;
    }
}
