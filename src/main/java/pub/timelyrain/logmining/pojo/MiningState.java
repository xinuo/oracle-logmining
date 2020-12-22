package pub.timelyrain.logmining.pojo;

public class MiningState {

    private long lastCommitScn = 0;
    private long lastRowNum = 0;
    private long lastSequence;
    private String lastTime;

    public MiningState() {
    }

    public MiningState(long lastCommitScn, long lastRowNum, long lastSequence, String lastTime) {
        this.lastCommitScn = lastCommitScn;
        this.lastRowNum = lastRowNum;
        this.lastSequence = lastSequence;
        this.lastTime = lastTime;
    }

    public long getLastRowNum() {
        return lastRowNum;
    }

    public void setLastRowNum(long lastRowNum) {
        this.lastRowNum = lastRowNum;
    }

    public long getLastCommitScn() {
        return lastCommitScn;
    }

    public void setLastCommitScn(long lastCommitScn) {
        this.lastCommitScn = lastCommitScn;
    }

    public long getLastSequence() {
        return lastSequence;
    }

    public void setLastSequence(long lastSequence) {
        this.lastSequence = lastSequence;
    }

    public String getLastTime() {
        return lastTime;
    }

    public void setLastTime(String lastTime) {
        this.lastTime = lastTime;
    }

    public void nextLog() {
        lastSequence++;
        lastRowNum = 0;
    }
}
