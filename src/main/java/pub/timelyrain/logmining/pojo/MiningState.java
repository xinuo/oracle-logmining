package pub.timelyrain.logmining.pojo;

public class MiningState {

    private long lastCommitScn;
    private int lastSequence;
    private String lastTime;
    private long startScn;

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
