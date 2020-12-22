package pub.timelyrain.logmining.biz;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import pub.timelyrain.logmining.config.Env;
import pub.timelyrain.logmining.pojo.MiningState;
import pub.timelyrain.logmining.pojo.RedoLog;
import pub.timelyrain.logmining.pojo.Row;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@Component
public class MiningService {
    Logger log = LogManager.getLogger(MiningService.class);


    private final JdbcTemplate jdbcTemplate;
    private final SQLExtractor sqlExtractor;
    private final RabbitTemplate rabbitTemplate;
    private final CounterService counterService;
    private final Env env;
    private final RedisTemplate redisTemplate;

    @Autowired
    public MiningService(JdbcTemplate jdbcTemplate, SQLExtractor sqlExtractor, RabbitTemplate rabbitTemplate, CounterService counterService, Env env, RedisTemplate redisTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.sqlExtractor = sqlExtractor;
        this.rabbitTemplate = rabbitTemplate;
        this.counterService = counterService;
        this.env = env;
        this.redisTemplate = redisTemplate;

    }

    private String miningSql;
    private MiningState state;
    private HashSet<String> traceTable = new HashSet<>();

    public void startMining() {
        // 读取state的seq
        state = loadState();
        // 判断 seq 小于数据库 archivelog的的最小 seq 提示有数据丢失（抓取程序长时间未启动，而归档日志被清理掉了）
        pollingCheck(state);
        miningSql = buildMiningSql();


        while (!Thread.interrupted()) {
            // 判断该seq是否是最末尾，若是代表本次抓取的日志不完整，需要再次抓取该文件。若不是末尾seq ，则一次可以抓取完整的日志。保存一个fulllog 状态.
            boolean completedLogFlag = checkCompletedLog(state);
            // 从 seq 开始读取 archive log 或 redo log
            pollingData(state);
            // 抓取完毕后,若fulllog=true,则seq+1 重复进行新日志抓取
            if (completedLogFlag)
                state.nextLog();

        }

    }

    /**
     * 检查当前的seq 是否是最新的日志,如果是最新的日志,表示该日志还可能被追加新的待抓取内容.需要重新读取
     *
     * @param state
     * @return
     */
    private boolean checkCompletedLog(MiningState state) {
        //检查归档日志的最大seq是否大于当前日志编号
        long maxSequence = jdbcTemplate.queryForObject("select max(sequence#) from v$archived_log", Long.class);
        if (maxSequence > state.getLastSequence())
            return true;
        //检查online日志的最大seq是否大于当前日志编号
        maxSequence = jdbcTemplate.queryForObject("select max(sequence#) from v$log", Long.class);
        if (maxSequence > state.getLastSequence())
            return true;

        return false;
    }

    /**
     * 检查数据库的日志列表是否完整,并警告
     *
     * @param state
     */
    private void pollingCheck(MiningState state) {
        long minSequence = jdbcTemplate.queryForObject("select min(sequence#) from v$archived_log", Long.class);
        if (minSequence > state.getLastSequence()) {
            log.warn("\r\n");
            log.warn("**********************************");
            log.warn("请注意: 最后一次处理的日志编号为 {} , 数据库保存最久的日志编号为 {}, 存在数据丢失的情况", state.getLastSequence(), minSequence);
            log.warn("**********************************");
            log.warn("\r\n");
        }
    }


    /**
     * 读取上次运行时的结束同步信息.用于恢复同步.
     *
     * @return
     */
    private MiningState loadState() {
        try {
            File stateFile = new File("state.saved");
            if (!stateFile.exists()) {
                //初次启动,读取当前seq.
                long seq = jdbcTemplate.queryForObject("select SEQUENCE# from v$log where status='CURRENT'", Long.class);
                log.info("未找到进度状态文件,从最新日志开始抓取,日志编号为 {}", seq);
                return new MiningState(0, 0, seq, null);
            }
            String stateStr = FileUtils.readFileToString(stateFile, "utf-8");
            ObjectMapper om = new ObjectMapper();
            MiningState lastState = om.readValue(stateStr, MiningState.class);
            log.info("读取状态文件信息为 {}", stateStr);
            return lastState;
        } catch (IOException e) {
            log.error("读取进度状态错误", e);
            throw new RuntimeException("读取进度状态错误");
        }
    }

    /**
     * 根据sequence 和 commitScn 抓取待同步数据.
     *
     * @param state
     * @return
     */
    private void pollingData(MiningState state) {
        //获得seq对应的日志文件的位置,先从归档日志里判断
        startLogFileMining(state);
        //启动日志分析
        jdbcTemplate.setFetchSize(500);
        jdbcTemplate.query(miningSql, (rs) -> {
            //读取事务id
            String xid = rs.getString("XID");
            int opCode = rs.getInt("OPERATION_CODE");
            long rn = rs.getLong("RN");
            //读取日志位置
            long scn = rs.getLong("SCN");
            long commitScn = rs.getLong("COMMIT_SCN");
            String timestamp = rs.getTimestamp("TIMESTAMP").toString();
            try {
                //log.debug(scn);
                //读取REDO
                int csf = rs.getInt("CSF");
                String schema = rs.getString("SEG_OWNER");
                String tableName = rs.getString("TABLE_NAME");
                String redo = rs.getString("SQL_REDO");
                String rowId = rs.getString("ROW_ID");
                if (csf == 1) {     //如果REDO被截断
                    while (rs.next()) {  //继续查询下一条REDO
                        redo += rs.getString("SQL_REDO");
                        if (0 == rs.getInt("CSF")) {  //
                            csf = 0;
                            break;  //退出循环
                        }
                    }
                }

                RedoLog redoLog = new RedoLog(schema, tableName, redo, rowId, scn, commitScn, timestamp, rn, xid, opCode);
                traceChange(redoLog);
            } finally {
                saveMiningState(commitScn, rn, state.getLastSequence(), timestamp);
            }
        }, state.getLastRowNum());   //传入redo value，不重复读取日志。
        //关闭日志分析
        log.debug("停止分析REDO日志 {}", Constants.MINING_END);
        jdbcTemplate.update(Constants.MINING_END);
    }

    private void startLogFileMining(MiningState state) {
        loadLogFile(state.getLastSequence());
//        for (int i = 1; i < env.getLogFileScaned(); i++) {
//            try {
//                loadLogFile(state.getLastSequence() - i);
//            } catch (Exception e) {
//                //为了避免日志切换造成数据丢失，同时加载两个连续的日志文件
//                //如果未找到前一个日志，不处理异常
//            }
//        }
        jdbcTemplate.update(Constants.MINING_START);

    }

    private void traceChange(RedoLog redoLog) {
        //判断是否是提交opCode=6 或回滚opCode=36
        if (7 == redoLog.getOpCode()) {
            commitRedo(redoLog);
            return;
        } else if (36 == redoLog.getOpCode()) {
            rollbackRedo(redoLog);
            return;
        } else if (1 == redoLog.getOpCode() || 2 == redoLog.getOpCode() || 3 == redoLog.getOpCode()) {
            saveRedo(redoLog);
            return;
        } else if (5 == redoLog.getOpCode()) {
            // todo 更新日志字典

        }
    }

    private void saveRedo(RedoLog redoLog) {
        if (!traceTable.contains(redoLog.getSchema() + "." + redoLog.getTableName()))
            return;
        String txKey = "mining:xid:" + redoLog.getXid();
        //新增、修改、删除
        redisTemplate.opsForList().rightPush(txKey, redoLog);
        log.debug("save redo to redis {}", redoLog.toString());
        //计数
        counterService.addCount();
    }

    private void commitRedo(RedoLog redoLog) {
        String txKey = "mining:xid:" + redoLog.getXid();
        //从redis中取得redolog的列表，并调用convertAndDelivery
        while (redisTemplate.opsForList().size(txKey) != 0) {
            RedoLog redo = (RedoLog) redisTemplate.opsForList().leftPop(txKey);
            convertAndDelivery(redo);
        }
        redisTemplate.delete(txKey);
    }

    private void rollbackRedo(RedoLog redoLog) {
        //根据事务id删除redis缓存的redelog
        redisTemplate.delete("mining:xid:" + redoLog.getXid());
        log.debug("识别回滚事务 事务id为 {}", redoLog.getXid());
    }

    private void loadLogFile(long sequence) {
        String logFile;
        boolean onlineLogFlag = false;
        List<Map<String, Object>> result = jdbcTemplate.queryForList("select name from v$archived_log where sequence# = ?", sequence);
        if (result.isEmpty()) {
            result = jdbcTemplate.queryForList("select f.member as name from v$log l inner join v$logfile f on l.group# = f.group# where sequence#= ?", sequence);
            onlineLogFlag = true;
        }
        Assert.state(!result.isEmpty(), String.format("未找到编号为 %d 的日志文件地址", sequence));

        logFile = (String) result.get(0).get("name");

        jdbcTemplate.update("begin dbms_logmnr.add_logfile(?);end;", logFile);
        log.debug("分析 {} 日志, 日志编号为 {}, 日志文件为 {}", (onlineLogFlag ? "online log" : "archive log"), sequence, logFile);
    }

    /**
     * 保存同步位置.用于下次程序运行时,可以恢复到停止点.
     *
     * @param commitScn
     * @param sequence
     * @param timestamp
     */
    private void saveMiningState(long commitScn, long redoValue, long sequence, String timestamp) {
        state.setLastCommitScn(commitScn);
        state.setLastRowNum(redoValue);
        state.setLastSequence(sequence);
        state.setLastTime(timestamp);

        String stateStr = null;
        try {
            File state = new File("state.saved");
            stateStr = new ObjectMapper().writeValueAsString(this.state);
            FileUtils.writeStringToFile(state, stateStr, "UTF-8", false);
//            log.debug("saved state {}", stateStr);
        } catch (Exception e) {
            log.error("写入传输状态错误,当前传输信息为 " + stateStr, e);
        }
    }

    private String buildMiningSqlWhere() {
        HashMap<String, String> tableGroup = new HashMap<>();
        //将数据库名.表名的数组，转成 数据库名和表名的集合
        String tb;
        for (String field : env.getTables()) {
            if (field.contains("|")) {
                tb = field.substring(0, field.indexOf("|"));
                tb = tb.toUpperCase();
                String condition = field.substring(field.indexOf("|") + 1, field.length());
                SQLExtractor.addRowCondition(tb, condition);
            } else {
                tb = field.toUpperCase();
            }

            Assert.isTrue(tb.contains("."), "同步表名格式不符合 数据库名.表名 的格式(" + tb + ")");
            Assert.isTrue(!tb.contains("\\*"), "同步表名格式不符合 不可以使用通配 *(" + tb + ")");
            traceTable.add(tb);
            String schemaName = tb.split("\\.")[0]; // 数据库名.表名
            String tableName = tb.split("\\.")[1]; // 数据库名.表名
            String tablesIn = tableGroup.get(schemaName);
            tablesIn = (tablesIn == null ? "'" + tableName + "'" : tablesIn + ",'" + tableName + "'");
            tableGroup.put(schemaName, tablesIn);
        }

        StringBuilder sql = new StringBuilder();

        for (String schemaName : tableGroup.keySet()) {
            if (sql.length() != 0) sql.append(" or ");

            String tablesIn = tableGroup.get(schemaName);
            if (tablesIn.startsWith("*"))
                sql.append(String.format(" SEG_OWNER = '%s' ", schemaName));
            else
                sql.append(String.format(" SEG_OWNER = '%s' and TABLE_NAME in (%s)", schemaName, tableGroup.get(schemaName)));
        }
        sql.insert(0, "( ").append(" )");

        return sql.toString();
    }

    private String buildMiningSql() {
//        StringBuilder sql = new StringBuilder();
//        sql.insert(0, buildMiningSqlWhere());
//        sql.insert(0, " AND ");
//        sql.insert(0, Constants.QUERY_REDO);
        String sql = String.format(Constants.QUERY_REDO, buildMiningSqlWhere());
        log.debug("日志查询的sql是 {}", sql.toString());
        return sql.toString();
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
            rabbitTemplate.convertAndSend(env.getExchangeName(), redoLog.getSchema() + "." + redoLog.getTableName(), row);

            log.debug("发送数据,类型{}.{} 表为{}.{}, sql is {}", row.getMode(), row.getOperator(), row.getSchemaName(), row.getTableName(), row.getSql());
        } catch (Exception e) {
            log.error("转换REDO和分发数据错误", e);
        }
    }


}
