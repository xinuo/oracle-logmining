package pub.timelyrain.logmining.biz;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import pub.timelyrain.logmining.pojo.MiningState;
import pub.timelyrain.logmining.pojo.Row;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Component
public class MiningDAO {
    Logger log = LogManager.getLogger(MiningDAO.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private SQLExtractor sqlExtractor;
    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Value("${mining.sync-table}")
    private String tables;
    @Value("${mining.polling-interval}")
    private int pollingInterval;
    @Value("${mining.polling-size}")
    private int pollingSize;
    @Value("${mining.init-start-time}")
    private String initStartTime;
    @Value("${mining.rabbit-exchange-name}")
    private String mqExchangeName;
    @Value("${mining.rabbit-routing-prefix}")
    private String routingPrefix;

    private String miningSql;
    private MiningState state;

    public void startMining() {
        //检查
        try {
            Assert.hasText(tables, "同步表名不能为空");
            Assert.isTrue(tables.contains("."), "同步表名需要为 数据库名.表名 的格式");
            long currentScn = loadDatabaseCurrentScn();
            state = loadStartScn();
            long startScn = state.getStartScn();
            Assert.isTrue(startScn <= currentScn, String.format("起始SCN %d 大于 当前SCN %d,请检查起始SCN配置", startScn, currentScn));

            miningSql = buildMiningSql();

            while (true) {
                long endScn = polling(startScn);
                startScn = endScn;
            }

        } catch (Exception e) {
            log.error("系统运行错误", e);
        }

    }

    private long polling(long startScn) throws InterruptedException {
        //查询数据库当前scn
        long currentScn = loadDatabaseCurrentScn();

        if (startScn == currentScn) {
            String msg = String.format("已捕获至日志末尾,暂停 %d 秒. (起始SCN %d 等于 当前SCN %d)", pollingInterval, startScn, currentScn);
            log.info(msg);
            TimeUnit.SECONDS.sleep(pollingInterval);
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
            Counter.addCount(scn, sequence);
            if (state.getLastCommitScn() >= commitScn && state.getLastSequence() >= sequence)
                return;

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

    private MiningState loadStartScn() throws IOException {
        //最优 读取上次停止的日志文件
        File stateFile = new File("state.saved");
        if (stateFile.exists()) {
            String stateStr = FileUtils.readFileToString(stateFile, "utf-8");
            ObjectMapper om = new ObjectMapper();
            MiningState lastState = om.readValue(stateStr, MiningState.class);
            log.info("从状态文件中读取上次执行的scn末位 {}", stateStr);

            if (initStartTime == null || "".equalsIgnoreCase(initStartTime)) {
                log.warn("发现上次分析截止commitScn为{} ，请输入归档日志检索的开始时间，以查找停止传输时是否存在未提交但需传输的数据。开始时间通过springboot启动参数 --mining.init-start-time=YYYYMMDDHH24MI 传入");
                System.exit(0);
            }
            //如果指定了开始时间，则查询开始时间对应的scn， 同时检索大于MiningState的commitScn的最小scn，优先用。
            jdbcTemplate.update(Constants.MINING_START_BY_TIME, initStartTime);
            long scn = jdbcTemplate.queryForObject(Constants.QUERY_MINING_START_SCN, Long.class, lastState.getLastCommitScn()).longValue();
            jdbcTemplate.update(Constants.MINING_END);

            if (scn < lastState.getStartScn()) {
                log.info("上次截止点 {} 前存在未传输数据，从 {} 开始本次传输", lastState.getStartScn(), scn);
                lastState.setStartScn(scn);
            } else {
                log.info("上次截止点 {} 前有没有未传输数据，从上次截止点开始本次传输", lastState.getStartScn());
            }
            return lastState;
        }

        MiningState lastState = new MiningState();
        //其次通过变更时间指定
        if (initStartTime != null && !"".equalsIgnoreCase(initStartTime)) {
            jdbcTemplate.update(Constants.MINING_START_BY_TIME, initStartTime);
            long scn = jdbcTemplate.queryForObject(Constants.QUERY_MINING_START_SCN, Long.class, 0).longValue();

            log.info("根据开始时间查询，开始传输scn为 {}", scn);
            lastState.setStartScn(scn);
            return lastState;
        }
        //根据当前时间
        long currentScn = loadDatabaseCurrentScn();
        log.info("以数据库当前时间，为开始传输的scn {}", currentScn);
        lastState.setStartScn(currentScn);

        return lastState;

    }

    private long loadDatabaseCurrentScn() {
        return jdbcTemplate.queryForObject(Constants.QUERY_CURRENT_SCN, Long.class);
    }

    private String buildMiningSql() {
        tables = tables.toUpperCase();
        String[] tbs = tables.split(",");
        HashMap<String, String> tableGroup = new HashMap<>();
        //将数据库名.表名的数组，转成 数据库名和表名的集合
        for (String tb : tbs) {
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
        sql.insert(0, " AND ( ").append(" )");
        sql.insert(0, Constants.QUERY_REDO);
        log.debug("日志查询的sql是 {}", sql.toString());
        return sql.toString();
    }

    private void convertAndDelivery(String schema, String tableName, String redo, String rowId, long scn, long commitScn, String timestamp) {
        try {
            Row row = sqlExtractor.parse(schema, tableName, redo);
            row.setRowId(rowId);
            row.setScn(scn);
            row.setCommitScn(commitScn);
            row.setTimestamp(timestamp);
            rabbitTemplate.convertAndSend(mqExchangeName, routingPrefix + "." + schema + "." + tableName, row);

            log.debug("发送数据,类型{}.{} 表为{}.{}, sql is {}", row.getMode(), row.getOperator(), row.getSchemaName(), row.getTableName(), row.getSql());
        } catch (Exception e) {
            log.error("转换REDO和分发数据错误", e);
        }
    }


}
