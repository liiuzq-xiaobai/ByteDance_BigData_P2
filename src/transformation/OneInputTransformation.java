package transformation;

import function.Function;
import operator.OneInputStreamOperator;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class OneInputTransformation<IN,OUT,OP extends Function> extends Transformation<IN,OUT> {
    //该转换的上游转换
    private Transformation<IN,OUT> input;

    private OneInputStreamOperator<IN,OUT,OP> operator;
}
