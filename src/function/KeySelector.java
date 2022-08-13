package function;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
public interface KeySelector<IN,KEY> extends Function {
    KEY getKey(IN value);
}
