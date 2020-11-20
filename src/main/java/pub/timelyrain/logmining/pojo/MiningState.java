package pub.timelyrain.logmining.pojo;

public class MiningState {

    private long lastCommitScn = 0;
    private int lastSequence;
    private String lastTime;
    private long lastTimestamp;
    private long startScn = 0;

    public long getLastCommitScn() {
        return lastCommitScn;
    }

    public void setLastCommitScn(long lastCommitScn) {
        this.lastCommitScn = lastCommitScn;
    }

    public int getLastSequence() {
        return lastSequence;
    }

    public void setLastSequence(int lastSequence) {
        this.lastSequence = lastSequence;
    }

    public long getStartScn() {
        return startScn;
    }

    public void setStartScn(long startScn) {
        this.startScn = startScn;
    }

    public String getLastTime() {
        return lastTime;
    }

    public void setLastTime(String lastTime) {
        this.lastTime = lastTime;
    }
}
