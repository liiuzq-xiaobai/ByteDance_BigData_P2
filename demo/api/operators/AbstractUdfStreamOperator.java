package api.operators;

import api.functions.Function;

public abstract class AbstractUdfStreamOperator<OUT, F extends Function> extends AbstractStreamOperator<OUT> implements OutputTypeConfigurable<OUT> {

}
