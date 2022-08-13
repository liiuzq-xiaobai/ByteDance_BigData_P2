package record;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class Tuple2<T0,T1> {
    public T0 f0;

    public T1 f1;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;

        if (f0 != null ? !f0.equals(tuple2.f0) : tuple2.f0 != null) return false;
        return f1 != null ? f1.equals(tuple2.f1) : tuple2.f1 == null;
    }

    @Override
    public int hashCode() {
        int result = f0 != null ? f0.hashCode() : 0;
        result = 31 * result + (f1 != null ? f1.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple2{" +
                "f0=" + f0 +
                ", f1=" + f1 +
                '}';
    }
}
