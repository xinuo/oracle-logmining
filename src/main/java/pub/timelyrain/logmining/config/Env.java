package pub.timelyrain.logmining.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.*;

@Configuration
@ConfigurationProperties(prefix = "mining")
public class Env {

    private boolean multiTenant;

    private String[] tables;

    private Map<String, List<String>> exchanges;

    private Map<String, String> routerInfo = new LinkedHashMap<>();
    private Map<String, String> conditionInfo = new LinkedHashMap<>();
    private Set<String> scanTables = new LinkedHashSet<>();

    public Map<String, List<String>> getExchanges() {
        return exchanges;
    }

    public void setExchanges(Map<String, List<String>> exchanges) {
        this.exchanges = exchanges;
    }

    public String[] getTables() {
        return tables;
    }

    public void setTables(String[] tables) {
        this.tables = tables;
    }

    public boolean isMultiTenant() {
        return multiTenant;
    }

    public void setMultiTenant(boolean multiTenant) {
        this.multiTenant = multiTenant;
    }


    public String init() {
        //初始化表与交换机关系
        for (int i = 0; i < tables.length; i++) {
            String[] tp = tables[i].split("\\|");
            if (tp.length < 2)
                throw new RuntimeException("未找到表对应的交换机设置");
            scanTables.add(tp[0]);
            routerInfo.put(tp[0], tp[1]);
            if (tp.length == 3) {
                //有跳转条件。
                conditionInfo.put(tp[0], tp[2]);
            }
        }
        return "Env{" +
                "multiTenant=" + multiTenant +
                ", tables=" + routerInfo +
                ", condition=" + conditionInfo +
                ", exchanges=" + exchanges +
                '}';
    }

    public String getExchangeName(String schema, String table) {
        return routerInfo.get(schema + "." + table);
    }

    public String getCondition(String schema, String table) {
        return conditionInfo.get(schema + "." + table);
    }

    public Set<String> getScanTables() {
        return scanTables;
    }
}
