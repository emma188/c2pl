import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;

public class CentralSite implements CentralSiteInterface{
    HashMap<Integer,DataSiteInterface> DataServices = new HashMap<>();
    int DataSiteNum = 0;
    CentralSiteInterface service;
    LockManager lockManager;
    public database db;
    int check = 4;
    LinkedList<Operation> grantOP = new LinkedList<>();
    public static void main(String[] args) throws RemoteException, MalformedURLException, SQLException, NotBoundException {
        CentralSite central = new CentralSite();
        central.dbInsertPrepare();
    }


    public CentralSite() throws RemoteException, MalformedURLException, NotBoundException {
        db = new database("central");
        try {
            try {
                LocateRegistry.createRegistry(Config.centralPort);
            } catch (RemoteException e) {
                System.out.println("Register the port "+Config.centralPort+ " failed:\n" + e.getMessage());
            }
            //Oldversion service = new Oldversion(0);
            service = (CentralSiteInterface) UnicastRemoteObject.exportObject(this, Config.centralPort);
            String centralURL = "rmi://" + Config.server_IP + ":" + Config.centralPort + "/" + Config.centralSiteName;
            Naming.rebind(centralURL, service);
            System.out.println("Central site is running...");
        } catch (Exception e){
            System.out.println("Central site startup failed!");
            e.printStackTrace();
        }
        lockManager = new LockManager(3);
    }

    public void ConnectToData(String dataURL, int siteID){
        try{
            DataSiteInterface service2 =  (DataSiteInterface)Naming.lookup(dataURL);
            DataServices.put(siteID,service2);
            DataSiteNum++;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void dbInsertPrepare() throws SQLException{
        String letter;
        for (int i = 0; i < 26; i++) {
            letter = String.valueOf((char)(97+i));
            db.inset(letter,i);
        }
    }

    public synchronized void sendOperationToLM(Operation op) throws SQLException, IOException {
        //System.out.println("-----central receive "+op.item+" from site "+op.siteID);
        Operation grant = lockManager.handleOp2(op);
//        if(grant != null){
//            if(op.type == Operation.COMMIT){
//                //System.out.println("send commit back to data site");
//            }
//            else {
//                //System.out.println(op.item + " from site " + op.siteID + " is granted");
//
//            }
//            //TODO:send back to data site
//        }
//        lockManager.printLockTable();
        //lockManager put operation to wait list or acquire list,
        //if acquire list, return acquire, else
        //if lockManager return acquire, return to data site
        //if lockManager return wait, do nothing
    }
    @Override
    public void establishConnection(String dataURL, int siteID) throws RemoteException {
        //System.out.println(dataURL);
        System.out.println("Data site "+siteID+" connected to central site");
        this.ConnectToData(dataURL,siteID);
    }

    @Override
    public synchronized LockMsg sendOperation(Operation op) throws IOException, SQLException {
        //sendOperationToLM(op);
        //System.out.println("1111");
//        if(op.type == Operation.READ){
//            System.out.println("receive R("+op.item+") from site "+op.siteID);
//        }
//        else {
//            System.out.println("receive W("+op.item+","+op.value+") from site "+op.siteID);
//        }
        Operation grant = lockManager.handleOp2(op);
        LockMsg lockMsg = new LockMsg(grant);
        return lockMsg;
    }

    @Override
    public synchronized void sendReleaseOp(Operation op) throws IOException, SQLException {
        if(op.type == Operation.WRITE || op.type == Operation.READ_WRITE){
            db.write(op.item, op.value);
            for (int i = 1; i <= DataServices.size(); i++) {
                if(i != op.siteID){
                    DataSiteInterface service = DataServices.get(i);
//                    if(op.type == Operation.READ){
//                        System.out.println("update R("+op.item+") to site "+op.siteID);
//                    }
//                    else {
//                        System.out.println("update W("+op.item+","+op.value+") to site "+op.siteID);
//                    }
                    String ack = service.sendUpdateMsg(op);
                    //System.out.println("receive");
                }
            }
        }
        LinkedList<Operation> acquireListToAdd =  lockManager.releaseOp(op);
//        if(op.type == Operation.READ){
//            System.out.println("Release R("+op.item+") from site "+op.siteID);
//        }
//        else {
//            System.out.println("Release W("+op.item+","+op.value+") from site "+op.siteID);
//        }

        if(acquireListToAdd.size() != 0){
            for (int i = 0; i < acquireListToAdd.size(); i++) {
                DataSiteInterface service = DataServices.get(acquireListToAdd.get(i).siteID);
                service.sendGrantLock(acquireListToAdd.get(i));
                lockManager.releaseWaitRelation(acquireListToAdd.get(i),op);
            }
        }

    }


    @Override
    public void sendCount(int count) throws RemoteException, SQLException {
        Oldversion d = new Oldversion(db); d.test(count);d.check(count);
    }

    @Override
    public void sendLMRply(Operation operation) throws IOException, SQLException {
        DataSiteInterface service = DataServices.get(operation.siteID);
        service.sendGrantLock(operation);
    }

    public boolean cmpOp(Operation op1, Operation op2){
        if(op1.siteID == op2.siteID
                && op1.transactionID == op2.transactionID
                && op1.type == op2.type
                && op1.item.compareTo(op2.item) == 0
                && op1.value == op2.value){
            return true;
        }
        return false;
    }

    public void sendReleaseToLM(){
        //先看是不是write， 如果是的话， 写入DB
        //lockManager把相应的op 从acquire list上删除，
        //      看看如果acquire list 空了
        //      or （wait list的第一个与剩下的 acquire list的siteID一样 and lock 可兼容）
        // 就把wait list中第一个放到 acquire list中， return给data site
        //如果 acquire list 没空，就单纯删除
    }
}
