package record;

/**
 * @author kevin.zeng
 * @description
 * @create 2022-08-12
 */
public class CheckPointBarrier extends StreamElement {
    //barrier唯一标识
    private int id;

    private static int idCounter=0;

    public CheckPointBarrier(){
        super();
        assignId();
    }

    private void assignId(){
        this.id = idCounter++;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "CheckPointBarrier{" +
                "id=" + id +
                ", taskId='" + taskId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CheckPointBarrier barrier = (CheckPointBarrier) o;

        return id == barrier.id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    public int getId() {
        return id;
    }
}

