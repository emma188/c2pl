import java.io.Serializable;

public class Operation implements Serializable {
    public static final int READ = 1;
    public static final int WRITE = 2;
    public static final int COMMIT = 3;
    public static final int READ_WRITE = 4;


    public int siteID;
    public int transactionID;
    public int type;
    public String item;
    public int value;
    //for read and write
    public Operation(int siteID, int transactionID, int type, String item, int value){
        this.siteID = siteID;
        this.transactionID = transactionID;
        this.type = type;
        this.item = item;
        this.value = value;
    }
    //for commit and abort
    public Operation(int siteID, int transactionID, int type){
        this.siteID = siteID;
        this.transactionID = transactionID;
        this.type = type;
    }
}
