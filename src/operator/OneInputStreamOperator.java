package operator;

import function.Function;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public abstract class OneInputStreamOperator<IN,OUT,F extends Function> extends StreamOperator<IN,OUT> {

    protected F userFunction;

    public OneInputStreamOperator(F userFunction) {
        this.userFunction = userFunction;
    }
}
