public class Lock {
    public static final int READ = 1;
    public static final int WRITE = 2;

    public String item;
    public int type;
    public int transactionID;
    public Lock(String item, int type, int transactionID){
        this.item = item;
        this.type = type;
        this.transactionID = transactionID;
    }
}
