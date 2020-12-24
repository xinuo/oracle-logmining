package pub.timelyrain.logmining.biz;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import pub.timelyrain.logmining.config.Env;
import pub.timelyrain.logmining.pojo.Row;
import pub.timelyrain.logmining.utils.ValueConverter;

import javax.script.*;
import java.util.*;

@Component
public class SQLExtractor {

    private static Logger log = LogManager.getLogger(SQLExtractor.class);

    public static final String MODE_DML = "DML";
    public static final String MODE_DDL = "DDL";
    public static final String OPERATOR_INSERT = "INSERT";
    public static final String OPERATOR_UPDATE = "UPDATE";
    public static final String OPERATOR_DELETE = "DELETE";

    private static HashMap<String, List> TABLE_DICT = new HashMap<>();
    private static HashMap<String, List> TABLE_PK = new HashMap<>();


    private static ScriptEngine ENGINE = new ScriptEngineManager().getEngineByName("ECMAScript");

    private JdbcTemplate jdbcTemplate;
    private Env env;

    @Autowired
    public SQLExtractor(JdbcTemplate jdbcTemplate, Env env) {
        this.jdbcTemplate = jdbcTemplate;
        this.env = env;
    }


    public Row parse(String schema, String table, String sql) throws JSQLParserException {
        Statement st = CCJSqlParserUtil.parse(sql);
        Row row;

        if (st instanceof Insert) {
            row = convInsert((Insert) st);
            if (!checkCondition(schema, table, row.getNewData())) return null;
        } else if (st instanceof Update) {
            row = convUpdate((Update) st);
            if (!checkCondition(schema, table, row.getNewData())) return null;
        } else if (st instanceof Delete) {
            row = convDelete((Delete) st);
            if (!checkCondition(schema, table, row.getOldData())) return null;
        } else {
            row = new Row();
            row.setMode(MODE_DDL);
            //如果发生ddl操作，需要清空该表的数据字典信息，以重新缓存
            log.debug("{}.{} 发生DDL,清空该表字典缓存", schema, table);
            TABLE_DICT.remove(schema + "." + table);
        }

        row.setSql(sql);
        row.setSchemaName(schema);
        row.setTableName(table);
        //查询数据字典
        if (!TABLE_DICT.containsKey(schema + "." + table)) {
            loadTableDict(schema, table);
        }
//        row.setStructure(TABLE_DICT.get(schema + "." + table));
        row.setPk(TABLE_PK.get(schema + "." + table));

        return row;
    }


    private boolean checkCondition(String schema, String tableName, Map<String, String> data) {
        String condition = env.getCondition(schema, tableName);
        if (condition == null)
            return true;

        HashMap<String, Object> dataObj = new HashMap();
        dataObj.putAll(data);
        ENGINE.setBindings(new SimpleBindings(dataObj), ScriptContext.ENGINE_SCOPE);
        try {
            boolean result = (boolean) ENGINE.eval(condition);
            log.debug("运行拉取条件 {}, 值为 {}", condition, result);
            return result;
        } catch (ScriptException e) {
            log.error("运行拉取条件错误 {} ", condition, e);
            return false;
        }
    }


    private void loadTableDict(String schema, String table) {
        List result = null;
        if (!env.isMultiTenant()) {
            result = jdbcTemplate.queryForList(Constants.QUERY_TALBE_DICT, schema, table);
        } else {
            String sql = Constants.QUERY_TALBE_DICT_CDB.replaceAll("\\$OWNER\\$", schema);
            sql = sql.replaceAll("\\$TABLE_NAME\\$", table);
            result = jdbcTemplate.queryForList(sql);
        }
        ArrayList<String> pkList = new ArrayList();
        for (Object o : result) {
            Map m = (Map) o;
            if ("Y".equalsIgnoreCase((String) m.get("PK")))
                pkList.add((String) m.get("COLUMN_NAME"));
        }
        TABLE_DICT.put(schema + "." + table, result);
        TABLE_PK.put(schema + "." + table, pkList);

    }

    private Row convInsert(Insert insert) {
        Row row = new Row();
        row.setOperator(OPERATOR_INSERT);
        row.setMode(MODE_DML);

//        row.setSchemaName(insert.getTable().getSchemaName());
//        row.setTableName(insert.getTable().getName());
        LinkedHashMap<String, String> newData = new LinkedHashMap<>();

        for (Column c : insert.getColumns()) {
            newData.put(parseValue(c.getColumnName()), null);
        }

        ExpressionList exp = (ExpressionList) insert.getItemsList();
        for (int i = 0; i < insert.getColumns().size(); i++) {
            String columnName = parseValue(insert.getColumns().get(i).getColumnName());
            String value = parseValue(exp.getExpressions().get(i).toString());

            newData.put(columnName, value);  //去除字段名和值 开头末尾的 单引号、双引号 null字符串 转为null等
        }
        row.setNewData(newData);

        return row;
    }

    private Row convUpdate(Update update) {
        Row row = new Row();
        row.setOperator(OPERATOR_UPDATE);
        row.setMode(MODE_DML);
//        row.setSchemaName(update.getTable().getSchemaName());
//        row.setTableName(update.getTable().getName());
        LinkedHashMap<String, String> newData = new LinkedHashMap<>();
        LinkedHashMap<String, String> oldData = new LinkedHashMap<>();

        update.getWhere().accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(final EqualsTo expr) {
                String columnName = parseValue(expr.getLeftExpression().toString());
                String value = parseValue(expr.getRightExpression().toString());
                oldData.put(columnName, value);

            }

            @Override
            public void visit(IsNullExpression expr) {
                String columnName = parseValue(expr.getLeftExpression().toString());
                oldData.put(columnName, null);
            }
        });

        for (Column c : update.getColumns()) {
            newData.put(parseValue(c.getColumnName()), null);
        }

        Iterator<Expression> iterator = update.getExpressions().iterator();

        for (String key : newData.keySet()) {
            Expression o = iterator.next();
            String value = parseValue(o.toString());
            newData.put(key, value);
        }

        //将未修改的数据复制到newdata内
        LinkedHashMap<String, String> allFieldNewData = new LinkedHashMap<>();
        allFieldNewData.putAll(oldData);
        allFieldNewData.putAll(newData);
        row.setNewData(allFieldNewData);
        row.setOldData(oldData);
        return row;
    }

    private Row convDelete(Delete delete) {
        Row row = new Row();
        row.setOperator(OPERATOR_DELETE);
        row.setMode(MODE_DML);
        LinkedHashMap<String, String> oldData = new LinkedHashMap<>();

        delete.getWhere().accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(final EqualsTo expr) {
                String columnName = parseValue(expr.getLeftExpression().toString());
                String value = parseValue(expr.getRightExpression().toString());
                oldData.put(columnName, value);

            }

            @Override
            public void visit(IsNullExpression expr) {
                String columnName = parseValue(expr.getLeftExpression().toString());
                oldData.put(columnName, null);
            }
        });

        return row;
    }

    private Row convDDL(Statement st) {
        Row row = new Row();
        row.setOperator(null);
        row.setMode(MODE_DDL);

        //row.setSchemaName();

        return row;
    }

    private String parseValue(String value) {
        if (value == null)
            return null;
        if (value.startsWith("'") && value.endsWith("'") || value.startsWith("\"") && value.endsWith("\""))
            return value.substring(1, value.length() - 1);
        if (value.equalsIgnoreCase("IS NULL") || value.equalsIgnoreCase("NULL"))
            return null;

        //判断入参是否是UNISTR('\4F55\4E16\6C11') 格式
        if (value.startsWith("UNISTR('") && value.endsWith("')")) {
            value = value.substring(8, value.length() - 2);
            return ValueConverter.unicodeToStr(value, "\\\\");
        }
        //判断入参是否是blob


        return value;
    }

}
