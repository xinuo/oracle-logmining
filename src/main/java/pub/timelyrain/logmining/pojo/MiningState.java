package pub.timelyrain.logmining.pojo;

public class MiningState {

    private long lastCommitScn = 0;
    private long lastSequence;
    private String lastTime;

    public MiningState() {
    }

    public MiningState(long lastCommitScn, long lastSequence, String lastTime) {
        this.lastCommitScn = lastCommitScn;
        this.lastSequence = lastSequence;
        this.lastTime = lastTime;
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
    }
}
