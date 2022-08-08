package api.operators;

import java.io.Serializable;

public interface StreamOperator<OUT> extends CheckpointListener, KeyContext, Disposable, Serializable {
    void open() throws Exception;

    void close() throws Exception;

    void dispose() throws Exception;

    void prepareSnapshotPreBarrier(long var1) throws Exception;

    OperatorSnapshotFutures snapshotState(long var1, long var3, CheckpointOptions var5, CheckpointStreamFactory var6) throws Exception;

    void initializeState(StreamTaskStateInitializer var1) throws Exception;
}
