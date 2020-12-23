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
    private long rowNum;
    private String xid;
    private int opCode;
    private String rsId;

    public RedoLog() {
    }

    public RedoLog(String schema, String tableName, String redo, String rowId, long scn, long commitScn, String timestamp, long rowNum, String xid, int opCode, String rsId) {
        this.schema = schema;
        this.tableName = tableName;
        this.redo = redo;
        this.rowId = rowId;
        this.scn = scn;
        this.commitScn = commitScn;
        this.timestamp = timestamp;
        this.rowNum = rowNum;
        this.xid = xid;
        this.opCode = opCode;
        this.rsId = rsId;
    }

    @Override
    public String toString() {
        return "RedoLog{" +
                "schema='" + schema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", rowId='" + rowId + '\'' +
                ", scn=" + scn +
                ", commitScn=" + commitScn +
                ", timestamp='" + timestamp + '\'' +
                ", rownum=" + rowNum +
                ", xid='" + xid + '\'' +
                ", opCode=" + opCode +
                '}';
    }

    public String getRsId() {
        return rsId;
    }

    public void setRsId(String rsId) {
        this.rsId = rsId;
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

    public long getRowNum() {
        return rowNum;
    }

    public void setRowNum(long rowNum) {
        this.rowNum = rowNum;
    }
}
