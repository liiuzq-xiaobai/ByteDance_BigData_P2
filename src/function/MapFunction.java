package function;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public interface MapFunction<IN,OUT> extends Function {

    OUT map(IN value);
}
