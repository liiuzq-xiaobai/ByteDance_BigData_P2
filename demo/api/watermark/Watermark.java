package api.watermark;

import runtime.streamrecord.StreamElement;

public final class Watermark extends StreamElement {
    public static final Watermark MAX_WATERMARK = new Watermark(Long.MAX_VALUE);
    public static final Watermark UNINITIALIZED = new Watermark(Long.MIN_VALUE);
    private final long timestamp;

    public Watermark(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public boolean equals(Object o) {
        return this == o || o != null && o.getClass() == Watermark.class && ((Watermark)o).timestamp == this.timestamp;
    }

    public int hashCode() {
        return (int)(this.timestamp ^ this.timestamp >>> 32);
    }

    public String toString() {
        return "Watermark @ " + this.timestamp;
    }
}
