package record;

/**
 * @author: liuzq
 * @date: 2022/8/17
 */
public class CheckPointRecord {
    private String operator;
    private String taskId;
    private String bufferPoolId;
    private String inputChannelId;
    private long offset;
    private String taskstatus;

    public CheckPointRecord() {
    }

    public CheckPointRecord(String operator, String taskId, String bufferPoolId, String inputChannelId, long offset, String taskstatus) {
        this.operator = operator;
        this.taskId = taskId;
        this.bufferPoolId = bufferPoolId;
        this.inputChannelId = inputChannelId;
        this.offset = offset;
        this.taskstatus = taskstatus;
    }

    public CheckPointRecord(String operator, String taskId, String bufferPoolId, long offset, String taskstatus) {
        this.operator = operator;
        this.taskId = taskId;
        this.bufferPoolId = bufferPoolId;
        this.offset = offset;
        this.taskstatus = taskstatus;
    }

    public CheckPointRecord(String operator, String taskId, long offset, String taskstatus) {
        this.operator = operator;
        this.taskId = taskId;
        this.offset = offset;
        this.taskstatus = taskstatus;
    }

    public CheckPointRecord(String source, String name, long bufferId, long offset, Thread.State state) {
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getBufferPoolId() {
        return bufferPoolId;
    }

    public void setBufferPoolId(String bufferPoolId) {
        this.bufferPoolId = bufferPoolId;
    }

    public String getInputChannelId() {
        return inputChannelId;
    }

    public void setInputChannelId(String inputChannelId) {
        this.inputChannelId = inputChannelId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getTaskstatus() {
        return taskstatus;
    }

    public void setTaskstatus(String taskstatus) {
        this.taskstatus = taskstatus;
    }

    @Override
    public String toString() {
        return "CheckPointRecord{" +
                "operator='" + operator + '\'' +
                ", taskId='" + taskId + '\'' +
                ", bufferPoolId='" + bufferPoolId + '\'' +
                ", inputChannelId='" + inputChannelId + '\'' +
                ", offset=" + offset +
                ", taskstatus='" + taskstatus + '\'' +
                '}' +"\n";
    }
}
