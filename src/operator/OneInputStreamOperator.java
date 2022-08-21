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

    //用户定义的函数处理逻辑
    protected F userFunction;
    //用户定义的key选择器，用于shuffle数据
    protected KeySelector<OUT,String> keySelector;
    //记录算子的当前计算状态
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
