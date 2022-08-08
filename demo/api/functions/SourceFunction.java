package api.functions;

import api.watermark.Watermark;

import java.io.Serializable;

public interface SourceFunction<T> extends Function, Serializable {
    void run(SourceContext<T> var1) throws Exception;

    void cancel();

    public interface SourceContext<T> {
        void collect(T var1);

        void collectWithTimestamp(T var1, long var2);

        void emitWatermark(Watermark var1);

        void markAsTemporarilyIdle();

        Object getCheckpointLock();

        void close();
    }
}
