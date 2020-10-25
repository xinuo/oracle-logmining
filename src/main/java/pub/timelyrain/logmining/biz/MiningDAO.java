package pub.timelyrain.logmining.biz;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import pub.timelyrain.logmining.sys.ShutdownHook;

import java.sql.PreparedStatement;

@Component
public class MiningDAO {
    private JdbcTemplate jdbcTemplate;

    private static final String START_MINING = "begin  SYS.DBMS_LOGMNR.START_LOGMNR(STARTSCN => ? ,OPTIONS =>  SYS.DBMS_LOGMNR.SKIP_CORRUPTION+SYS.DBMS_LOGMNR.NO_SQL_DELIMITER+SYS.DBMS_LOGMNR.NO_ROWID_IN_STMT+SYS.DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + SYS.DBMS_LOGMNR.CONTINUOUS_MINE+SYS.DBMS_LOGMNR.COMMITTED_DATA_ONLY+SYS.DBMS_LOGMNR.STRING_LITERALS_IN_STMT);  end;";
    private static final String QUERY_REDO = "SELECT thread#, scn, start_scn, nvl(commit_scn,scn) commit_scn ,(xidusn||'.'||xidslt||'.'||xidsqn) AS xid,timestamp, operation_code, operation,status, SEG_TYPE_NAME ,info,seg_owner, table_name, username, sql_redo ,row_id, csf, TABLE_SPACE, SESSION_INFO, RS_ID, RBASQN, RBABLK, SEQUENCE#, TX_NAME, SEG_NAME, SEG_TYPE_NAME FROM  v$logmnr_contents  WHERE OPERATION_CODE in (1,2,3,5) and nvl(commit_scn,scn)>=? and ((SEG_OWNER='CIICFS_TEST'))";

    @Autowired
    public MiningDAO(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void startMining() {

        int currentSCN = 1522547;
        jdbcTemplate.update(START_MINING, currentSCN);

        jdbcTemplate.setFetchSize(1);
        jdbcTemplate.query((con) -> {
            PreparedStatement ps = con.prepareStatement(QUERY_REDO);
            ps.setInt(1, currentSCN);
            ShutdownHook.addRunningStatement(ps);
            return ps;
        }, (rs) -> {
            while (rs.next()) {
                System.out.println(rs.getInt("SCN") + "\t" + rs.getString("SQL_REDO"));
            }
        });


    }


}
