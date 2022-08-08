package api.operators;

import java.io.Serializable;

public interface StreamOperatorFactory<OUT> extends Serializable {
    void setChainingStrategy(ChainingStrategy var1);

    ChainingStrategy getChainingStrategy();

    default boolean isStreamSource() {
        return false;
    }

    default boolean isLegacySource() {
        return false;
    }

    default boolean isOutputTypeConfigurable() {
        return false;
    }

    default void setOutputType(TypeInformation<OUT> type, ExecutionConfig executionConfig) {
    }

    default boolean isInputTypeConfigurable() {
        return false;
    }

    default void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
    }

    Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader var1);
}
