package pub.timelyrain.logmining.sys;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

public class ShutdownHook extends Thread {
    public static List<PreparedStatement> RUNNING_STATEMENTS;

    public static void addRunningStatement(PreparedStatement ps) {
        if (RUNNING_STATEMENTS == null)
            RUNNING_STATEMENTS = new ArrayList<>();

        RUNNING_STATEMENTS.add(ps);
    }

    @Override
    public void run() {
        System.out.println("system shuting down");
        for(PreparedStatement ps : RUNNING_STATEMENTS){
            try {
                if (!ps.isClosed()) {
                    ps.cancel();
                }
            }catch (Exception e){

            }
        }
    }
}
