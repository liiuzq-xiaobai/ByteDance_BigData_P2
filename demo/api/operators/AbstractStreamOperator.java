package api.operators;

import java.io.Serializable;

public abstract class AbstractStreamOperator<OUT> implements StreamOperator<OUT>, SetupableStreamOperator<OUT>, StreamOperatorStateHandler.CheckpointedStreamOperator, Serializable {

}
