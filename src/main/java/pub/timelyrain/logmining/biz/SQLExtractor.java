package pub.timelyrain.logmining.biz;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
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
import pub.timelyrain.logmining.pojo.Row;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

@Component
public class SQLExtractor {
    private static Logger log = LogManager.getLogger(SQLExtractor.class);

    public static final String MODE_DML = "DML";
    public static final String MODE_DDL = "DML";
    public static final String OPERATOR_INSERT = "INSERT";
    public static final String OPERATOR_UPDATE = "UPDATE";
    public static final String OPERATOR_DELETE = "DELETE";

    private static HashMap<String, List> TABLE_DICT = new HashMap<>();

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public SQLExtractor(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Row parse(String schema, String table, String sql) throws JSQLParserException {
        Statement st = CCJSqlParserUtil.parse(sql);
        Row row;

        if (st instanceof Insert) {
            row = convInsert((Insert) st);
        } else if (st instanceof Update) {
            row = convUpdate((Update) st);
        } else if (st instanceof Delete) {
            row = convDelete((Delete) st);
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
        row.setStructure(TABLE_DICT.get(schema + "." + table));

        return row;
    }

    private void loadTableDict(String schema, String table) {
        List result = jdbcTemplate.queryForList(Constants.QUEYR_TALBE_DICT, schema, table);
        TABLE_DICT.put(schema + "." + table, result);
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
        });

        newData.putAll(oldData);

        for (Column c : update.getColumns()) {
            newData.put(parseValue(c.getColumnName()), null);
        }

        Iterator<Expression> iterator = update.getExpressions().iterator();

        for (String key : newData.keySet()) {
            Expression o = iterator.next();
            String value = parseValue(o.toString());
            newData.put(key, value);
        }

        row.setNewData(newData);
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

        return value;
    }

}
