package pub.timelyrain.logmining.biz;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import pub.timelyrain.logmining.config.Env;
import pub.timelyrain.logmining.pojo.MiningState;
import pub.timelyrain.logmining.pojo.Row;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Component
public class MiningService {
    Logger log = LogManager.getLogger(MiningService.class);

    private final JdbcTemplate jdbcTemplate;
    private final SQLExtractor sqlExtractor;
    private final RabbitTemplate rabbitTemplate;
    private final CounterService counterService;
    private final Env env;

    @Autowired
    public MiningService(JdbcTemplate jdbcTemplate, SQLExtractor sqlExtractor, RabbitTemplate rabbitTemplate, CounterService counterService, Env env) {
        this.jdbcTemplate = jdbcTemplate;
        this.sqlExtractor = sqlExtractor;
        this.rabbitTemplate = rabbitTemplate;
        this.counterService = counterService;
        this.env = env;

    }

    private String miningSql;
    private MiningState state;

    public void startMining() {
        //检查
        try {
            state = loadState();
            if (state.getStartScn() == 0) {
                long currentScn = loadDatabaseCurrentScn();
                state.setStartScn(currentScn);
                state.setLastCommitScn(currentScn);
            }
            //如果startscn ！= commitscn。说明是正常同步时记录的scn，检查这个 scn区段间是否有其他事务当时正在执行，确定最小的起点scn
            state = checkRestartScn(state);


            miningSql = buildMiningSql();
            long startScn = state.getStartScn();
            while (true) {
                long endScn = polling(startScn);
                startScn = endScn;
            }

        } catch (Exception e) {
            log.error("系统运行错误", e);
        }

    }

    /**
     * 恢复同步时,检查停止时是否有正在同步的事务,需要补传.
     *
     * @param state
     * @return
     */
    private MiningState checkRestartScn(MiningState state) {
        //如果startscn ！= commitscn。说明是正常同步时记录的scn，检查这个 scn区段间是否有其他事务当时正在执行，确定最小的起点scn
        if (state.getStartScn() == state.getLastCommitScn())
            return state;

        log.info("查询同步任务结束时是否有正在进行中的事务需要同步数据");
        log.info("\t查询scn段内的事务情况");
        jdbcTemplate.update("begin SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => ?, ENDSCN => ?, OPTIONS =>  SYS.DBMS_LOGMNR.SKIP_CORRUPTION + + SYS.DBMS_LOGMNR.CONTINUOUS_MINE + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG);  end;", state.getStartScn(), state.getLastCommitScn());
        //查询并行的事务id
        List result = jdbcTemplate.queryForList("select distinct rawtohex(xid) as xid from v$logmnr_contents where scn >= ? and scn <= ?", state.getStartScn(), state.getLastCommitScn());
        jdbcTemplate.update(Constants.MINING_END);

        if (result.isEmpty()) {
            log.info("检查没有发现需要补传的数据");
            return state;
        }

        StringBuilder sqlwhere = new StringBuilder();
        for (Object o : result) {
            Map m = (Map) o;
            sqlwhere.append("'").append(m.get("xid")).append("',");
        }
        if (sqlwhere.length() != 0)
            sqlwhere.delete(sqlwhere.length() - 1, sqlwhere.length());

        sqlwhere.insert(0, "select min(nvl(scn,0)) as minscn , count(xid) as xidcount from v$logmnr_contents where operation = 'COMMIT' and xid in (").append(")");
        log.debug("查询并行事务最小scn {}", sqlwhere.toString());

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        Date seekEndTime = new Date();
        try {
            seekEndTime = sdf.parse(state.getLastTime());
        } catch (ParseException e) {
            throw new RuntimeException("计算时间错误", e);
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(seekEndTime);
        while (true) {
            //计算后延10分钟后的时间点
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            calendar.add(Calendar.MINUTE, 10);
            String endTime = sdf2.format(calendar.getTime());
            log.info("\t查询从 {} 截止至 {}", state.getStartScn(), endTime);

            //根据并行的事务id，查询所有事务的最小scn
            jdbcTemplate.update("begin SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => ?, ENDTIME => LEAST(TO_DATE(?,'YYYY-MM-DD HH24:MI:SS'),SYSDATE), OPTIONS => SYS.DBMS_LOGMNR.SKIP_CORRUPTION + SYS.DBMS_LOGMNR.CONTINUOUS_MINE + SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG);  end;", state.getStartScn(), endTime);
            List result2 = jdbcTemplate.queryForList(sqlwhere.toString());
            jdbcTemplate.update(Constants.MINING_END);
            if (result.isEmpty()) {
                log.warn("** 未找到事务信息，忽略同步未提交事务数据 **");
                break;
            }
            Map info = (Map) result2.get(0);
            BigDecimal xidCount = (BigDecimal) info.get("xidcount");
            BigDecimal minScn = (BigDecimal) info.get("minscn");
            if (minScn.longValue() != 0 && xidCount.intValue() != result.size()) {
                log.info("\t已提交事务数为 {},最小scn为 {}, 需要继续查询最小scn", xidCount.intValue(), minScn.longValue());
                continue;
            }
            log.info("查询到最小scn, startSCN从 {} 修改为 {}", state.getStartScn(), minScn.longValue());
            state.setStartScn(minScn.longValue());
            break;
        }

        return state;
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
                return new MiningState();
            }

            String stateStr = FileUtils.readFileToString(stateFile, "utf-8");
            ObjectMapper om = new ObjectMapper();
            MiningState lastState = om.readValue(stateStr, MiningState.class);
            log.info("从状态文件中读取上次执行的scn末位 {}", stateStr);
            return lastState;
        } catch (IOException e) {
            log.error("读取进度状态错误", e);
            throw new RuntimeException("读取进度状态错误");
        }
    }

    /**
     * 根据起始scn抓取待同步数据.
     *
     * @param startScn 起始同步的数据库变更点.
     * @return
     * @throws InterruptedException
     */
    private long polling(long startScn) throws InterruptedException {
        //查询数据库当前scn
        long currentScn = loadDatabaseCurrentScn();

        if (startScn == currentScn) {
            String msg = String.format("已捕获至日志末尾,暂停 %d 秒. (起始SCN %d 等于 当前SCN %d)", env.getInterval(), startScn, currentScn);
            log.info(msg);
            TimeUnit.SECONDS.sleep(env.getInterval());
        }
        //计算分析一次归档的终点scn，性能调优参数，在一次分析的范围大小与数据库压力之间调整，
        //endScn不应大于数据库最大scn值
//        long endScn = Math.min(startScn + pollingSize, currentScn);
        long endScn[] = new long[1];
        endScn[0] = startScn;
        //启动日志分析
        log.info("开始分析REDO日志 " + Constants.MINING_START_ENDLESS.replaceAll("\\?", "{}"), startScn);
        jdbcTemplate.update(Constants.MINING_START_ENDLESS, startScn);
        jdbcTemplate.setFetchSize(1);

        log.debug("提取REDO日志 {}", miningSql);
        jdbcTemplate.query(miningSql, (rs) -> {
            //读取日志位置
            long scn = rs.getLong("SCN");
            long commitScn = rs.getLong("COMMIT_SCN");
            int sequence = rs.getInt("SEQUENCE#");
            String timestamp = rs.getTimestamp("TIMESTAMP").toString();
            //计数
            counterService.addCount(scn, sequence);

            if (state.getLastCommitScn() >= commitScn && state.getLastSequence() >= sequence) {
                log.debug("忽略已同步数据 commitscn为 {}", commitScn);
                return;
            }
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
                if (csf == 0) {
                    log.debug(redo);
                    convertAndDelivery(schema, tableName, redo, rowId, scn, commitScn, timestamp);
                } else {
                    log.warn("REDO log 处于截断状态 SCN:{} ,REDO SQL {}", scn, redo);
                }
            } finally {
                saveMiningState(scn, commitScn, sequence, timestamp);
                endScn[0] = scn;
            }

        });

        //关闭日志分析
        log.debug("停止分析REDO日志 {}", Constants.MINING_END);
        jdbcTemplate.update(Constants.MINING_END);
        return endScn[0];  //返回本次结束的scn，作为下一次轮询的startScn
    }

    /**
     * 保存同步位置.用于下次程序运行时,可以恢复到停止点.
     *
     * @param scn
     * @param commitScn
     * @param sequence
     * @param timestamp
     */
    private void saveMiningState(long scn, long commitScn, int sequence, String timestamp) {
        state.setStartScn(scn);
        state.setLastCommitScn(commitScn);
        state.setLastSequence(sequence);
        state.setLastTime(timestamp);

        String stateStr = null;
        try {
            File state = new File("state.saved");
            stateStr = new ObjectMapper().writeValueAsString(this.state);
            FileUtils.writeStringToFile(state, stateStr, "UTF-8", false);
            log.debug("saved state {}", stateStr);
        } catch (Exception e) {
            log.error("写入传输状态错误,当前传输信息为 " + stateStr, e);
        }
    }

//    /**
//     * @return
//     * @throws IOException
//     * @Depre
//     */
//    private MiningState loadStartScn() throws IOException {
//        //最优 读取上次停止的日志文件
//        File stateFile = new File("state.saved");
//        if (stateFile.exists()) {
//            String stateStr = FileUtils.readFileToString(stateFile, "utf-8");
//            ObjectMapper om = new ObjectMapper();
//            MiningState lastState = om.readValue(stateStr, MiningState.class);
//            log.info("从状态文件中读取上次执行的scn末位 {}", stateStr);
//
//            if (env.getInitStartTime() == null || "".equalsIgnoreCase(env.getInitStartTime())) {
//                log.warn("发现上次分析截止commitScn为{} ，请输入归档日志检索的开始时间，以查找停止传输时是否存在未提交但需传输的数据。开始时间通过springboot启动参数 --mining.init-start-time=YYYYMMDDHH24MI 传入");
//                System.exit(0);
//            }
//            //如果指定了开始时间，则查询开始时间对应的scn， 同时检索大于MiningState的commitScn的最小scn，优先用。
//            jdbcTemplate.update(Constants.MINING_START_BY_TIME, env.getInitStartTime());
//            long scn = jdbcTemplate.queryForObject(Constants.QUERY_MINING_START_SCN, Long.class, lastState.getLastCommitScn()).longValue();
//            jdbcTemplate.update(Constants.MINING_END);
//
//            if (scn < lastState.getStartScn()) {
//                log.info("上次截止点 {} 前存在未传输数据，从 {} 开始本次传输", lastState.getStartScn(), scn);
//                lastState.setStartScn(scn);
//            } else {
//                log.info("上次截止点 {} 前有没有未传输数据，从上次截止点开始本次传输", lastState.getStartScn());
//            }
//            return lastState;
//        }
//
//        MiningState lastState = new MiningState();
//        //其次通过变更时间指定
//        if (env.getInitStartTime() != null && !"".equalsIgnoreCase(env.getInitStartTime())) {
//            jdbcTemplate.update(Constants.MINING_START_BY_TIME, env.getInitStartTime());
//            long scn = jdbcTemplate.queryForObject(Constants.QUERY_MINING_START_SCN, Long.class, 0).longValue();
//
//            log.info("根据开始时间查询，开始传输scn为 {}", scn);
//            lastState.setStartScn(scn);
//            return lastState;
//        }
//        //根据当前时间
//        long currentScn = loadDatabaseCurrentScn();
//        log.info("以数据库当前时间，为开始传输的scn {}", currentScn);
//        lastState.setStartScn(currentScn);
//
//        return lastState;
//
//    }

    private long loadDatabaseCurrentScn() {
        return jdbcTemplate.queryForObject(Constants.QUERY_CURRENT_SCN, Long.class);
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
            String schemaName = tb.split("\\.")[0]; // 数据库名.表名
            String tableName = tb.split("\\.")[1]; // 数据库名.表名
            String tablesIn = tableGroup.get(schemaName);
            if (tableName.contains("*")) {
                log.debug("{}设置为捕获全部表.", schemaName);
                tablesIn = "*";
            } else {
                tablesIn = (tablesIn == null ? "'" + tableName + "'" : tablesIn + ",'" + tableName + "'");
                log.debug("拼装同步表名的过滤sql {}->{}", schemaName, tablesIn);
            }
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
        StringBuilder sql = new StringBuilder();
        sql.insert(0, buildMiningSqlWhere());
        sql.insert(0, " AND ");
        sql.insert(0, Constants.QUERY_REDO);
        log.debug("日志查询的sql是 {}", sql.toString());
        return sql.toString();
    }

    private void convertAndDelivery(String schema, String tableName, String redo, String rowId, long scn, long commitScn, String timestamp) {
        try {
            Row row = sqlExtractor.parse(schema, tableName, redo);
            if (row == null)
                return;

            row.setRowId(rowId);
            row.setScn(scn);
            row.setCommitScn(commitScn);
            row.setTimestamp(timestamp);
            rabbitTemplate.convertAndSend(env.getExchangeName(), schema + "." + tableName, row);

            log.debug("发送数据,类型{}.{} 表为{}.{}, sql is {}", row.getMode(), row.getOperator(), row.getSchemaName(), row.getTableName(), row.getSql());
        } catch (Exception e) {
            log.error("转换REDO和分发数据错误", e);
        }
    }


}
