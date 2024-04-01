import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.*;
import java.io.*;
public class DataSite implements Runnable,DataSiteInterface{
    public static int GlobalSiteId = 0;
    public static String centralURL = "rmi://" + Config.server_IP + ":" + Config.centralPort + "/" + Config.centralSiteName;
    private int SiteId;//start from 1
    private String dataSiteURL;
    public int countSite;
    public int count = 1;
    CentralSiteInterface service2;
    public HashMap<Integer, ArrayList<Operation>> transactions = new HashMap<>();
    public int transactionNum = 0;
    public database db;
    Boolean lockGranted = false;
    Boolean block;

    public static void main(String[] args) throws RemoteException, SQLException {
        DataSite d1 = new DataSite();
        DataSite d2 = new DataSite();
        DataSite d3 = new DataSite();
        int testNum = 4;
        test(testNum,d1,d2,d3);
        d1.dbInsertPrepare();
        d2.dbInsertPrepare();
        d3.dbInsertPrepare();
        Thread t1 = new Thread(d1);
        Thread t2 = new Thread(d2);
        Thread t3 = new Thread(d3);
        t1.start();
        t2.start();
        t3.start();
    }
    public static void test(int testNum, DataSite d1, DataSite d2, DataSite d3) throws SQLException {
        if(testNum == 1) {
//            d1.readTransaction("/Users/zhenhuansu/Desktop/c2pl/src/test1-testResult1.txt");
//            d2.readTransaction("/Users/zhenhuansu/Desktop/c2pl/src/test1-testResult2.txt");
//            d3.readTransaction("/Users/zhenhuansu/Desktop/c2pl/src/test1-testResult3.txt");
            d1.readTransaction("src/test1-1.txt");
            d2.readTransaction("src/test1-2.txt");
            d3.readTransaction("src/test1-3.txt");
        }
        else if(testNum == 2) {
            d1.readTransaction("src/test2-1.txt");
            d2.readTransaction("src/test2-2.txt");
            d3.readTransaction("src/test3-3.txt");
        }
        else if(testNum == 3) {
            d1.readTransaction("src/test3-1.txt");
            d2.readTransaction("src/test3-2.txt");
            d3.readTransaction("src/test3-3.txt");
        }
        else if(testNum == 4) {
            d1.readTransaction("src/test4-1.txt");
            d2.readTransaction("src/test4-2.txt");
            d3.readTransaction("src/test4-3.txt");
        }
        loadDB(testNum,d1,d2,d3);
    }



    public DataSite(){
        //this.name = name;
        block = false;
        GlobalSiteId++;
        SiteId = GlobalSiteId;
        db = new database(Config.dataSiteName+SiteId);
        dataSiteURL = "rmi://" + Config.server_IP + ":" + "123"+SiteId + "/"+Config.dataSiteName+SiteId;
        int port = 1230+SiteId;
        try{
            try {
                LocateRegistry.createRegistry(port);
            }
            catch (RemoteException e) {
                System.out.println("Register the port "+port+ " failed:\n" + e.getMessage());
            }
            //Oldversion service = new Oldversion(SiteId);
            DataSiteInterface service = (DataSiteInterface) UnicastRemoteObject.exportObject(this, port);
            Naming.rebind(dataSiteURL, service);
            System.out.println("Date site "+SiteId+" is running...");
        }catch (Exception e){
            System.out.println("Data site "+SiteId+ " startup failed!");
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

    public void ConnectToCentral(){
        try{
            service2 =  (CentralSiteInterface)Naming.lookup(centralURL);
            System.out.println("Data site "+SiteId+" establishing connection...");
            //System.out.println(dataSiteURL);
            service2.establishConnection(dataSiteURL,SiteId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void loadDB(int testNum, DataSite d1, DataSite d2, DataSite d3) throws SQLException {
        d1.countSite = testNum;
        //d1.writeDB(testNum);d2.writeDB(testNum);d3.writeDB(testNum);
    }

    public void readTransaction(String fileName){
        try {
            BufferedReader in = new BufferedReader(new FileReader(fileName));
            String str;
            ArrayList<Operation> opSet = new ArrayList<>();
            while ((str = in.readLine()) != null) {
                if(str.equals("Transaction")){
                    //transactionNum++;
                    //transactions.put(transactionNum, new ArrayList<>());
                }
                else if(str.startsWith("w(") && str.startsWith("r(")){
                    int start = str.indexOf('w')+2;
                    int mid = str.indexOf(',');
                    int end = str.indexOf(')');
                    String item = str.substring(start,mid);
                    String value = str.substring(mid+1,end);
                    int v = Integer.parseInt(value);
                    Operation op = new Operation(SiteId,transactionNum,Operation.READ_WRITE,item,v);
                    opSet.add(op);
                }
                else if(str.startsWith("r(")){
                    int start = str.indexOf('(')+1;
                    int end = str.indexOf(')');
                    String item = str.substring(start,end);
                    Operation op = new Operation(SiteId,transactionNum,Operation.READ,item,-1);
                    opSet.add(op);
                }
                else if(str.startsWith("w(")){
                    int start = str.indexOf('(')+1;
                    int mid = str.indexOf(',');
                    int end = str.indexOf(')');
                    String item = str.substring(start,mid);
                    String value = str.substring(mid+1,end);
                    int v = Integer.parseInt(value);
                    Operation op = new Operation(SiteId,transactionNum,Operation.WRITE,item,v);
                    opSet.add(op);
                }
                else if(str.equals("commit")){
                    Operation op = new Operation(SiteId,transactionNum,Operation.COMMIT);
                    opSet.add(op);
                    transactions.put(transactionNum,opSet);
                    opSet = new ArrayList<>();
                }
            }
            //System.out.println(str);
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void printTestResult(int num) throws IOException, SQLException {
        String fileName = "src/testResult"+num+".txt";
        BufferedReader in = new BufferedReader(new FileReader(fileName));
        String str;
        Oldversion d = new Oldversion(db);d.check(countSite);
        service2.sendCount(countSite);
        while ((str = in.readLine()) != null) {
            System.out.println(str);
        }
    }



    //for each transaction and each operation inside this transaction, send to central site
    public void requestLock() throws IOException, SQLException, InterruptedException {
        if(transactionNum == 0){
            return;
        }
        for (int i = 1; i <= transactionNum; i++) {
            ArrayList<Operation> opSet = transactions.get(i);
            ArrayList<Operation> writeSet = new ArrayList<>();
            for (int j = 0; j < opSet.size(); j++) {
                Operation op = opSet.get(j);
//                if(op.type == Operation.READ) {
//                    System.out.println("site " + SiteId + " send R("+op.item+")");
//                }
//                else{
//                    System.out.println("site " + SiteId + " send W("+op.item+","+op.value+")");
//                }
                LockMsg lockMsg = service2.sendOperation(op);
                //System.out.println("1111");
                //service2.sendOperation(op);
                lockGranted = false;
                while (op.type == Operation.COMMIT && lockMsg.op == null){
//                    System.out.println("site "+SiteId+" start wait");
//                    while (true){
//                        //System.out.println("wait");
//                        if(lockGranted == true){
//                            break;
//                        }
//                    }

                    //Thread.sleep(10);
                    //blocked();
//                    System.out.println("site "+SiteId+" stop wait");
                    service2.sendOperation(op);
                }
//                else{
//                    //send to DB + send release msg
//                    if(lockMsg.op.type == Operation.READ){
//                        int value = db.read(lockMsg.op.item);
//                        System.out.println("Site "+lockMsg.op.siteID+" read "+lockMsg.op.item+" = "+value);
//                    }
//                    else if (lockMsg.op.type == Operation.WRITE){
//                        db.write(lockMsg.op.item, lockMsg.op.value);
//                        System.out.println("Site "+lockMsg.op.siteID+" write "+lockMsg.op.item+" = "+lockMsg.op.value);
//                    }
//                    service2.sendReleaseOp(lockMsg.op);
//                }
            }
            //System.out.println("transaction finished");
            //write Update To DB after commit;
            //writeUpdateToDB(writeSet);
            //writeSet = new ArrayList<>();
        }
    }

//    public void writeUpdateToDB(ArrayList<Operation> writeSet) throws SQLException{
//        Operation op;
//        for (int i = 0; i < writeSet.size(); i++) {
//            op = writeSet.get(i);
//            db.write(op.item,op.value);
//            System.out.println("Data site "+SiteId+" write ("+op.item+") = "+op.value);
//        }
//    }



    @Override
    public void run() {
        ConnectToCentral();
        try {
            requestLock();
            if(SiteId == count){
                printTestResult(countSite);
            }
        } catch (RemoteException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Override
    public synchronized void sendGrantLock(Operation op) throws IOException, SQLException {
        //System.out.println(op.type);
        if(op.type == Operation.READ){
            int value = db.read(op.item);
            System.out.println("Site "+op.siteID+" read "+op.item+" = "+value);
        }
        else if (op.type == Operation.WRITE){
            System.out.println("Site "+op.siteID+" write "+op.item+" = "+op.value);
            db.write(op.item, op.value);
        }
        else if(op.type == Operation.READ_WRITE){
            int value = db.read(op.item);
            db.write(op.item, value);
            System.out.println("Site "+op.siteID+" read and write "+op.item+" = "+op.value);
        }
        //unblocked();
        service2.sendReleaseOp(op);

    }

    @Override
    public synchronized String sendUpdateMsg(Operation op) throws RemoteException, SQLException {
        db.write(op.item, op.value);
        return "ack";
    }


    public void unblocked(){
        block = false;
    }

}
