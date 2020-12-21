package pub.timelyrain.logmining.pojo;

public class RedoLog {
    //schema, tableName, redo, rowId, scn, commitScn, timestamp
    private String schema;
    private String tableName;
    private String redo;
    private String rowId;
    private long scn;
    private long commitScn;
    private String timestamp;
    private long redoValue;
    private String xid;
    private int opCode;

    public RedoLog(String schema, String tableName, String redo, String rowId, long scn, long commitScn, String timestamp, long redoValue, String xid, int opCode) {
        this.schema = schema;
        this.tableName = tableName;
        this.redo = redo;
        this.rowId = rowId;
        this.scn = scn;
        this.commitScn = commitScn;
        this.timestamp = timestamp;
        this.redoValue = redoValue;
        this.xid = xid;
        this.opCode = opCode;
    }

    public int getOpCode() {
        return opCode;
    }

    public void setOpCode(int opCode) {
        this.opCode = opCode;
    }

    public String getXid() {
        return xid;
    }

    public void setXid(String xid) {
        this.xid = xid;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRedo() {
        return redo;
    }

    public void setRedo(String redo) {
        this.redo = redo;
    }

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public long getScn() {
        return scn;
    }

    public void setScn(long scn) {
        this.scn = scn;
    }

    public long getCommitScn() {
        return commitScn;
    }

    public void setCommitScn(long commitScn) {
        this.commitScn = commitScn;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public long getRedoValue() {
        return redoValue;
    }

    public void setRedoValue(long redoValue) {
        this.redoValue = redoValue;
    }
}
