package record;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
public class StreamElement {
    public final boolean isRecord() {
        return getClass() == StreamRecord.class;
    }

    public final <T> StreamRecord<T> asRecord() {
        return (StreamRecord<T>) this;
    }

    public final boolean isWatermark() {
        return getClass() == Watermark.class;
    }

    public final Watermark asWatermark() {
        return (Watermark) this;
    }

    // 判断是否是checkPoint
    public final boolean isCheckPointBarrier(){
        return getClass() == CheckPointBarrier.class;
    }

    // 转换成CheckPointBarrier类型
    public final CheckPointBarrier asCheckPointBarrier() {
        return (CheckPointBarrier) this;
    }
}
