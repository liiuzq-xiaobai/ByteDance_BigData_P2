package runtime.streamrecord;

public abstract class StreamElement {
    public StreamElement() {
    }

//    public final boolean isWatermark() {
//        return this.getClass() == Watermark.class;
//    }
//
//    public final boolean isStreamStatus() {
//        return this.getClass() == StreamStatus.class;
//    }
//
//    public final boolean isRecord() {
//        return this.getClass() == StreamRecord.class;
//    }
//
//    public final boolean isLatencyMarker() {
//        return this.getClass() == LatencyMarker.class;
//    }
//
//    public final <E> StreamRecord<E> asRecord() {
//        return (StreamRecord)this;
//    }
//
//    public final Watermark asWatermark() {
//        return (Watermark)this;
//    }
//
//    public final StreamStatus asStreamStatus() {
//        return (StreamStatus)this;
//    }
//
//    public final LatencyMarker asLatencyMarker() {
//        return (LatencyMarker)this;
//    }
}
