package pub.timelyrain.logmining.pojo;

import java.util.HashMap;

public class Row {
    private String schemaName;
    private String tableName;
    /**
     * DML,DDL
     */
    private String mode;
    private String Sql;

    /**
     * INSERT,UPDATE,DELETE
     */
    private String operator;
    private HashMap<String, String> newData;
    private HashMap<String, String> oldData;
    private HashMap<String, String> structure;

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSql() {
        return Sql;
    }

    public void setSql(String sql) {
        Sql = sql;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public HashMap<String, String> getNewData() {
        return newData;
    }

    public void setNewData(HashMap<String, String> newData) {
        this.newData = newData;
    }

    public HashMap<String, String> getOldData() {
        return oldData;
    }

    public void setOldData(HashMap<String, String> oldData) {
        this.oldData = oldData;
    }

    public HashMap<String, String> getStructure() {
        return structure;
    }

    public void setStructure(HashMap<String, String> structure) {
        this.structure = structure;
    }
}
