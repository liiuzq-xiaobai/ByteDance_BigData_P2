package operator;

import common.KeyedState;
import function.Function;
import function.KeySelector;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public abstract class OneInputStreamOperator<IN,OUT,F extends Function> extends StreamOperator<IN,OUT> {

    protected F userFunction;

    //有些算子需要按key进行聚合，如reduce，因此map中需要声明下发数据的选择器
    //TODO 这里强制把key设为了String
    protected KeySelector<OUT,String> keySelector;

    protected KeyedState<String, OUT> valueState;

    public OneInputStreamOperator(F userFunction) {
        this(userFunction,null);
    }

    public OneInputStreamOperator(F userFunction,KeySelector<OUT,String> keySelector){
        this.userFunction = userFunction;
        this.keySelector = keySelector;
    }


    public void setValueState(KeyedState<String, OUT> valueState) {
        this.valueState = valueState;
    }

    public KeyedState<String, OUT> getValueState() {
        return valueState;
    }
}
