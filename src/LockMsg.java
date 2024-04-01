import java.io.Serializable;

public class LockMsg implements Serializable {
    public Operation op;
    public LockMsg(Operation op){
        this.op = op;
    }
}
