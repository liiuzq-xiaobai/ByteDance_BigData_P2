package environment;


import java.lang.reflect.Method;

/**
 * @author kevin.zeng
 * @description 提供给用户的运行环境
 * @create 2022-08-12
 */
public class StreamExecutionEnvironment {

    private final String mainProgramClasspath = "main.MainProgram";

    //用户调用，执行wordcount程序
    public void execute() {
        Class mainClass = null;
        try {
            mainClass = Class.forName(mainProgramClasspath);
            Method main = mainClass.getDeclaredMethod("main", String[].class);
            //当用反射调用方法时，如果目标方法的入参是一个数组，则要把数组包装到另一个Object数组中
            main.invoke(null,new Object[]{new String[0]});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}