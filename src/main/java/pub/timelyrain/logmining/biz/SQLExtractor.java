package pub.timelyrain.logmining.biz;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;
import pub.timelyrain.logmining.pojo.Row;

import java.util.Enumeration;
import java.util.HashMap;

public class SQLExtractor {
    public static final String MODE_DML = "DML";
    public static final String MODE_DDL = "DML";
    public static final String OPERATOR_INSERT = "INSERT";
    public static final String OPERATOR_UPDATE = "UPDATE";
    public static final String OPERATOR_DELETE = "DELETE";


    public static Row parse(String sql) throws JSQLParserException {
        Statement st = CCJSqlParserUtil.parse(sql);

        if (st instanceof Insert) {
            return convInsert((Insert) st);
        } else if (st instanceof Update) {
            return convUpdate((Update) st);
        } else if (st instanceof Delete) {
            return convDelete((Delete) st);
        } else {
            return convDDL(st);
        }

    }

    private static Row convInsert(Insert insert) {
        Row row = new Row();
        row.setOperator(OPERATOR_INSERT);
        row.setMode(MODE_DML);

        row.setSchemaName(insert.getTable().getSchemaName());
        row.setTableName(insert.getTable().getName());
        HashMap<String, String> newData = new HashMap<>();

        ExpressionList exp = (ExpressionList) insert.getItemsList();

        for (int i = 0; i < insert.getColumns().size(); i++) {
            String columeName = insert.getColumns().get(i).getColumnName();
            String value = exp.getExpressions().get(i).toString();

            newData.put(parseValue(columeName), parseValue(value));
        }
        row.setNewData(newData);

        return row;
    }

    private static Row convUpdate(Update update) {
        Row row = new Row();
        row.setOperator(OPERATOR_UPDATE);
        row.setMode(MODE_DML);


        return row;
    }

    private static Row convDelete(Delete delete) {
        Row row = new Row();
        row.setOperator(OPERATOR_DELETE);
        row.setMode(MODE_DML);


        return row;
    }

    private static Row convDDL(Statement st) {
        Row row = new Row();
        row.setOperator(null);
        row.setMode(MODE_DDL);

        //row.setSchemaName();

        return row;
    }

    private static String parseValue(String value) {
        if (value == null)
            return null;
        if (value.startsWith("'") && value.endsWith("'") || value.startsWith("\"") && value.endsWith("\""))
            return value.substring(1, value.length() - 1);
        if (value.equalsIgnoreCase("IS NULL") || value.equalsIgnoreCase("NULL"))
            return null;

        return value;
    }


    public static void main(String[] args) throws Exception {
        String insert = " insert into \"CIICFS_TEST\".\"A001\"(\"A001001\",\"A001007\",\"A001011\",\"A001041\",\"A001044\",\"A001077\",\"A001207\",\"A001225\",\"A001705\",\"ID\",\"A001728\",\"A001735\",\"A001730\",\"A001738\",\"A001999\",\"A001743\",\"A001745\",\"A001753\",\"A001755\",\"A001202\",\"A001234\",\"A001216\",\"A001217\",\"A001243\",\"A001244\",\"A001245\",\"A001246\",\"A001709\",\"A001205\",\"NUMS\",\"CREATE_TIME\",\"LAST_UPDATE_TIME\",\"LAST_OPERATOR\",\"A001213\",\"A001220\",\"A001222\",\"A001701\",\"BATCH_NO\",\"CUS_ID\",\"A001488\",\"A001489\",\"A001204\",\"A001208\",\"A001211\",\"A001223\",\"A001224\",\"A001227\",\"A001228\",\"A001230\",\"A001231\",\"A001702\",\"A001700\",\"A001703\",\"A001704\",\"A001707\",\"A001725\",\"CREATER\",\"A001706\",\"A001708\",\"A001000\",\"A001501\",\"A001502\",\"A001503\",\"A001504\",\"A001505\",\"A001002\",\"BRANCH_ID\",\"ACCOUNT_ID\",\"A001403\",\"A001756\",\"A001710\",\"A001711\",\"A001712\",\"A001713\",\"A001715\",\"A001716\",\"A001717\",\"A001718\",\"A001719\",\"A001720\",\"A001721\",\"A001722\",\"A001723\",\"A001724\",\"A001726\",\"A001727\",\"A001729\",\"A001731\",\"A001732\",\"A001733\",\"A001734\",\"A001736\",\"A001737\",\"A001739\",\"A001740\",\"A001741\",\"A001742\",\"A001744\",\"A001141\",\"A001142\",\"A001143\",\"A001144\",\"A001145\",\"A001146\",\"A001147\",\"A001148\",\"A001149\",\"A001150\",\"A001151\",\"A001152\",\"A001153\",\"A001154\",\"A001155\",\"A001156\",\"A001157\",\"A001158\",\"A001159\",\"A001160\",\"A001161\",\"A001162\",\"A001163\",\"A001164\",\"A001165\",\"A001166\",\"A001167\",\"A001168\",\"A001169\",\"A001170\",\"UNIT_ID\",\"A001247\",\"A001248\",\"A001714\",\"A001800\",\"A001801\",\"A001802\",\"A001803\",\"A001804\",\"A001805\",\"A001806\",\"A001807\",\"A001808\",\"A001809\",\"A001810\",\"A001811\",\"A001812\",\"A001813\",\"A001814\",\"A001815\",\"A001816\",\"A001817\",\"A001818\",\"A001819\",\"A001820\",\"A001821\",\"A001822\",\"A001823\",\"A001824\",\"A001825\",\"A001826\",\"A001827\",\"A001828\",\"A001829\",\"A001830\",\"A001831\",\"A001832\",\"A001833\",\"A001834\",\"A001835\",\"A001836\",\"A001837\") values ('史莉','01002','1979-09-26',NULL,NULL,'220582197909261320',NULL,NULL,'9999007279','40288a945e708f07016049261eb144d8','00014253',NULL,'00900','00014253',NULL,'9999007279',NULL,NULL,'00900',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'3018000344',NULL,NULL,'2017-12-12 13:15:23','2017-12-12 13:15:23','40062567',NULL,NULL,NULL,'9999007279',NULL,'9999007279',NULL,NULL,NULL,'31598300011',NULL,NULL,'3167821046','00900','3152830151','3153000026',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'40062567',NULL,'00900',NULL,NULL,NULL,NULL,NULL,NULL,NULL,'cw','40288a945e708f07016049261eb144d8',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'67EB7DFC329FC00AE050A8C005097592',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'318601')";

        Statement stmt = CCJSqlParserUtil.parse(insert);

        Row row = convInsert((Insert) stmt);

        System.out.println(row.getSchemaName());
        System.out.println(row.getSchemaName());

    }
}
