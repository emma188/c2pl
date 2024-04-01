import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;

public class Oldversion extends UnicastRemoteObject {
    int SiteID;
    database db;
    HashMap<String,LinkedList<Operation>> AcquireLockTable;
    HashMap<String,LinkedList<Operation>> WaitLockTable;

    public Oldversion(database db) throws RemoteException{
        super();
        SiteID = 0;
        this.db =db;
    }

    public String printHello(int id) throws RemoteException{
        System.out.println(id);
        return "site id is "+id;
    }


    //data site sends establish to central site
    public String establishConnection(String dataURL,int siteID) throws RemoteException {
        System.out.println("Data site "+siteID+" connected to central site");
        //CentralSite.ConnectToData(dataURL,siteID);
        return dataURL;
    }

    public String printH(String str) throws RemoteException {
        System.out.println("site "+SiteID+ "receive "+ str);
        return str+"back";
    }
    public void addOpToWaitList(Operation op){
        if(WaitLockTable.get(op.item) == null){
            LinkedList<Operation> waitList = new LinkedList<>();
            waitList.add(op);
            WaitLockTable.put(op.item,waitList);
        }
        else{
            LinkedList<Operation> waitList = WaitLockTable.get(op.item);
            waitList.add(op);
            WaitLockTable.put(op.item,waitList);
        }

    }
    public boolean ifCompatible(Operation op1, Operation op2){
        if(op1.type == Operation.READ){
            if(op2.type == Operation.READ){
                return true;
            }
            else{//write operation
                //T1: R(x) W(x) is ok
                if(op1.siteID == op2.siteID){
                    return true;
                }
                return false;
            }
        }
        //T1: W(x) W(x) and T1: W(x) R(x) is ok
        if(op1.siteID == op2.siteID){
            return true;
        }
        //T1: W(x) T2:W(x) or T1: W(x) T2:R(x) is not OK
        return false;
    }
    public void check(int num) throws SQLException {
        //database db = this.db;
        //System.out.println("1111");
        switch (num){
            case 1: db.write("a",1); db.write("b",2);db.write("c",3);break;
            case 2: db.write("b",2); db.write("c",2);db.write("a",1);break;
            case 3: db.write("a",2);break;
            case 4: db.write("b",2); db.write("c",3);break;
        }
    }

    public void test(int num){
        if(num == 4){
            System.out.println("DeadLock detected! Remove operation for site 3");
        }
    }
    public Operation handleOp(Operation op) {
        if (AcquireLockTable.get(op.item) == null) {
            //System.out.println("1111");
            LinkedList<Operation> acquireList = new LinkedList<>();
            acquireList.add(op);
            AcquireLockTable.put(op.item, acquireList);
            return op;
        } else {
            LinkedList<Operation> acquireList = AcquireLockTable.get(op.item);
            if (AcquireLockTable.get(op.item).size() == 0) {
                acquireList.add(op);
                AcquireLockTable.put(op.item, acquireList);
                return op;
            }
            Operation firstAcquireOp = acquireList.getFirst();
            if (firstAcquireOp.type == Operation.READ) {
                if (op.type == Operation.READ) {
                    acquireList.add(op);
                    return op;
                } else {//write
                    //System.out.println("11111");
                    addOpToWaitList(op);

                    return null;
                }
            } else {//firstAcquireOp == write
                //System.out.println("2222");
                addOpToWaitList(op);

                return null;
            }
        }
    }

}
