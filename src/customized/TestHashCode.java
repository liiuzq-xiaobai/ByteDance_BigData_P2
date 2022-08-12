package customized;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-13
 */
public class TestHashCode {
    public static void main(String[] args) {
        int size = 2;
//        compareHashCode("flink")
    }

    static boolean compareHashCode(String s1,String s2,int size){
        return s1.hashCode() % size == s2.hashCode() % size;
    }
}
