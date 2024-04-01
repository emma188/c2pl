import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.*;

public class LockManager {
    HashMap<String,LinkedList<Operation>> AcquireLockTable;
    HashMap<String,LinkedList<Operation>> WaitLockTable;
    HashMap<Integer, LinkedList<Operation>> transactions;
    int [][] edges;
    int n = 0;
    int countCommit = 0;
    CentralSiteInterface service;
    int dataSiteNum;
    HashMap<Integer,Boolean> firstCommit = new HashMap<>();
    public LockManager(int dataSiteNum) throws MalformedURLException, NotBoundException, RemoteException {
        service =  (CentralSiteInterface) Naming.lookup(DataSite.centralURL);
        System.out.println("lock manager establishing connection...");
        this.dataSiteNum = dataSiteNum;
        for (int i = 0; i < dataSiteNum; i++) {
            firstCommit.put(i,false);
        }
        AcquireLockTable = new HashMap<>();
        WaitLockTable = new HashMap<>();
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

    public Operation handleOp(Operation op){
        if(AcquireLockTable.get(op.item) == null){
            //System.out.println("1111");
            LinkedList<Operation> acquireList = new LinkedList<>();
            acquireList.add(op);
            AcquireLockTable.put(op.item,acquireList);
            return op;
        }
        else{
            LinkedList<Operation> acquireList = AcquireLockTable.get(op.item);
            if(AcquireLockTable.get(op.item).size() == 0){
                acquireList.add(op);
                AcquireLockTable.put(op.item, acquireList);
                return op;
            }
            Operation firstAcquireOp = acquireList.getFirst();
            if(firstAcquireOp.type == Operation.READ){
                if(op.type == Operation.READ){
                    acquireList.add(op);
                    return op;
                }
                else {//write
                    //System.out.println("11111");
                    addOpToWaitList(op);
                    edges[op.siteID][firstAcquireOp.siteID] = 1;
                    n++;
                    if(bfsToFindCycle(n,edges) == false){
                        System.out.println("DeadLock detected! Remove operation for site "+op.siteID);
                        abort(op.siteID);
                    }
                    return null;
                }
            }
            else{//firstAcquireOp == write
                //System.out.println("2222");
                addOpToWaitList(op);
                edges[op.siteID][firstAcquireOp.siteID] = 1;
                n++;
                if(bfsToFindCycle(n,edges) == false){
                    System.out.println("DeadLock detected! Remove operation for site "+op.siteID);
                    abort(op.siteID);
                }
                return null;
            }
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
    public boolean ifCompatible1(Operation op){
        LinkedList<Operation> acquireList = AcquireLockTable.get(op.item);
        Boolean hasWrite = false;
        int siteIDForWrite = -1;
        for (int i = 0; i < acquireList.size(); i++) {
            if(acquireList.get(i).type == Operation.WRITE || acquireList.get(i).type == Operation.READ_WRITE){
                hasWrite = true;
                siteIDForWrite = acquireList.get(i).siteID;
                break;
            }
        }
        if(hasWrite == false){
            if(op.type == Operation.READ){
                return true;
            }
            else{//write type
                for (int i = 0; i < acquireList.size(); i++) {
                    //A：1r 2r, op = 1w is not ok
                    if(acquireList.get(i).siteID != op.siteID){
                        return false;
                    }
                }
                //A：1r 2r, op = 1r is ok
                return true;
            }
        }
        else { //AcquireLockTable has write
            //A：1r 1w, op = 1w or 1r is ok
            if(siteIDForWrite == op.siteID){
                return true;
            }
            //A：1r 1w, op = 2w or 2r is not ok
            return false;
        }
    }

    public Operation handleOp2(Operation op) throws SQLException, IOException {
        if(op.type == Operation.COMMIT){
            Operation op1 = handleCommit(op);
            return op1;
        }
        //item doesn't have lock
        if(transactions.get(op.siteID) == null){
            LinkedList<Operation> opSet = new LinkedList<>();
            opSet.add(op);
            transactions.put(op.siteID,opSet);
        }
        else {
            LinkedList<Operation> opSet = transactions.get(op.siteID);
            opSet.add(op);
            transactions.put(op.siteID,opSet);
        }
        if(AcquireLockTable.get(op.item) == null){
            //System.out.println("1111");
            LinkedList<Operation> acquireList = new LinkedList<>();
            acquireList.add(op);
            AcquireLockTable.put(op.item,acquireList);
            return null;
        }
        else{//item has lock
            LinkedList<Operation> acquireList = AcquireLockTable.get(op.item);
            if(AcquireLockTable.get(op.item).size() == 0){
                acquireList.add(op);
                AcquireLockTable.put(op.item, acquireList);
                return null;
            }
            //System.out.println("2222");
            //acquireList.get(0);
            if(ifCompatible1(op) == true){
                //System.out.println("3333");
                acquireList.add(op);
                AcquireLockTable.put(op.item, acquireList);
                return null;
            }
            else {//lock incompatible
                if(WaitLockTable.get(op.item) == null){
                    //System.out.println("4444");
                    LinkedList<Operation> waitList = new LinkedList<>();
                    Operation oplast = acquireList.getLast();
                    waitList.add(op);
                    WaitLockTable.put(op.item, waitList);
                    edges[op.siteID][oplast.siteID] = 1;
                    n++;
                    if(bfsToFindCycle(n,edges) == false){
                        System.out.println("DeadLock detected! Remove operation for site "+op.siteID);
                        abort(op.siteID);
                    }
                }
                else {
                    //System.out.println("5555");
                    LinkedList<Operation> waitList = WaitLockTable.get(op.item);
                    Operation oplast = waitList.getLast();
                    waitList.add(op);
                    WaitLockTable.put(op.item, waitList);
                    edges[op.siteID][oplast.siteID] = 1;
                    n++;
                    if(bfsToFindCycle(n,edges) == false){
                        System.out.println("DeadLock detected! Remove operation for site "+op.siteID);
                        abort(op.siteID);
                    }
                }
            }
        }
        return null;
    }

    public synchronized Operation handleCommit(Operation op) throws SQLException, IOException {
        if(firstCommit.get(op.siteID) == false){
            firstCommit.put(op.siteID,true);
            return null;
        }
        if(isWaiting(op.siteID)==true){
            return null;
        }
        for (int i = 0; i < transactions.get(i).size(); i++) {
            unlock(transactions.get(i).remove(i));
        }
        return op;
//        int siteID = op.siteID;
//        Boolean ifRemove = false;
//        Set<String> itemSet = AcquireLockTable.keySet();
//        Iterator<String> it = itemSet.iterator();
//        while (it.hasNext()){
//            //for each item
//            String item = it.next();
//            //remove op related to siteID from AcquireLockTable
//            LinkedList<Operation> acquireList = AcquireLockTable.get(item);
//            LinkedList<Operation> acquireRemoveList = new LinkedList<>();
//            Operation op1;
//            if(acquireList != null) {
//                //System.out.println("size "+acquireList.size());
//                Iterator<Operation> acquireIt = acquireList.iterator();
//                while (acquireIt.hasNext()){
//                    op1 = acquireIt.next();
//                    if(op1.siteID == siteID){
//                        acquireRemoveList.add(op1);
//                    }
//                }
//                if(acquireRemoveList.size() != 0){
//                    ifRemove = true;
//                    for (int i = 0; i < acquireRemoveList.size(); i++) {
//                        acquireList.remove(acquireRemoveList.get(i));
//                    }
//                    AcquireLockTable.put(item,acquireList);
//                }
//            }
//            //remove op related to siteID from WaitLockTable
//            LinkedList<Operation> waitList = WaitLockTable.get(item);
//            LinkedList<Operation> waitRemoveList = new LinkedList<>();
//            Operation op2;
//            if(waitList != null) {
//                Iterator<Operation> waitIt = waitList.iterator();
//                while (waitIt.hasNext()){
//                    op2 = waitIt.next();
//                    if(op2.siteID == siteID){
//                        waitRemoveList.add(op2);
//                    }
//                }
//            }
//            if(waitRemoveList.size() != 0){
//                ifRemove = true;
//                for (int i = 0; i < waitRemoveList.size(); i++) {
//                    waitList.remove(waitRemoveList.get(i));
//                }
//                WaitLockTable.put(item,waitList);
//            }
        //}
//        if(ifRemove) {
//            modifyLockTable(siteID);
//        }
    }

    public void modifyLockTable(int siteID){
        //      看看如果acquire list 空了
        //      or （wait list的第一个与剩下的 acquire list的siteID一样 and lock 可兼容）
        // 就把wait list中第一个放到 acquire list中， return给data site
        //如果 acquire list 没空，就单纯删除
        Set<String> itemSet = AcquireLockTable.keySet();
        Iterator<String> it = itemSet.iterator();
        while (it.hasNext()){
            //for each item
            String item = it.next();
            LinkedList<Operation> acquireList = AcquireLockTable.get(item);
            LinkedList<Operation> waitList = WaitLockTable.get(item);
            //if acquire list is empty
            if(acquireList.size() == 0){
                if(waitList != null && waitList.size() != 0){
                    LinkedList<Operation> waitRemoveList = new LinkedList<>();
                    Operation firstWaitOp = waitList.getFirst();
                    waitList.add(firstWaitOp);
                    if(firstWaitOp.type == Operation.WRITE || firstWaitOp.type == Operation.READ_WRITE){
                        for (int i = 1; i < waitList.size(); i++) {
                            if(waitList.get(i).siteID != firstWaitOp.siteID){
                                break;
                            }
                            waitRemoveList.add(waitList.get(i));
                        }
                    }
                    else{ // first wait operation is read
                        for (int i = 1; i < waitList.size(); i++) {
                            if(waitList.get(i).siteID != firstWaitOp.siteID){

                            }
                        }
                    }
                    for (int i = 1; i < waitList.size(); i++) {
                        if(firstWaitOp.type == Operation.WRITE || firstWaitOp.type == Operation.READ_WRITE){

                        }
                    }
                }
                //TODO:send back to data site
            }
        }
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

    public LinkedList<Operation> releaseOp(Operation op){
        LinkedList<Operation> acquireList = AcquireLockTable.get(op.item);
        for (int i = 0; i < acquireList.size(); i++) {
            if(cmpOp(acquireList.get(i),op)){
                acquireList.remove(i);
                break;
            }
        }
        AcquireLockTable.put(op.item,acquireList);
        //System.out.println(AcquireLockTable.get(op.item).size());
        //modify lock table
        return modifyLockTable(op);
    }

    public LinkedList<Operation> modifyLockTable(Operation op){
        LinkedList<Operation> acquireList = AcquireLockTable.get(op.item);
        LinkedList<Operation> waitList = WaitLockTable.get(op.item);
        LinkedList<Operation> acquireListToAdd = new LinkedList<>();
        LinkedList<Integer> waitListToRemove = new LinkedList<>();
        if(waitList == null){
            return acquireListToAdd;
        }
        Operation firstWaitOp = waitList.getFirst();
        if(firstWaitOp == null){
            return acquireListToAdd;
        }
        if(acquireList.size() == 0){
            if(firstWaitOp.type == Operation.READ){
                for (int i = 0; i < waitList.size(); i++) {
                    if(waitList.get(i).type == Operation.WRITE || waitList.get(i).type == Operation.READ_WRITE){
                        break;
                    }
                    acquireList.add(waitList.get(i));
                    waitListToRemove.add(i);
                    acquireListToAdd.add(waitList.get(i));
                    edges[waitList.get(i).siteID][op.siteID] = 0;
                    n--;
                }
                if(bfsToFindCycle(n,edges) == false){
                    System.out.println("DeadLock detected! Remove operation for site "+op.siteID);
                    abort(op.siteID);
                }
            }
            else{
                acquireList.add(firstWaitOp);
                waitListToRemove.add(0);
                acquireListToAdd.add(firstWaitOp);
                edges[firstWaitOp.siteID][op.siteID] = 0;
                n--;
                if(bfsToFindCycle(n,edges) == false){
                    System.out.println("DeadLock detected! Remove operation for site "+op.siteID);
                    abort(op.siteID);
                }
            }
        }
        else{
            if(firstWaitOp.type == Operation.READ){
                for (int i = 0; i < waitList.size(); i++) {
                    if(waitList.get(i).type == Operation.WRITE || waitList.get(i).type == Operation.READ_WRITE){
                        break;
                    }
                    acquireList.add(waitList.get(i));
                    waitListToRemove.add(i);
                    acquireListToAdd.add(waitList.get(i));
                    edges[waitList.get(i).siteID][op.siteID] = 0;
                    n--;
                }
                if(bfsToFindCycle(n,edges) == false){
                    System.out.println("DeadLock detected! Remove operation for site "+op.siteID);
                    abort(op.siteID);
                }
            }
        }
        if(acquireListToAdd.size() != 0){
            AcquireLockTable.put(op.item,acquireList);
        }
        if(waitListToRemove.size() != 0){
            for (int i = 0; i < waitListToRemove.size(); i++) {
                waitList.remove(waitListToRemove.get(i));
            }
            WaitLockTable.put(op.item, waitList);
        }
        return acquireListToAdd;
    }

    public synchronized void printLockTable(){
        Set<String> itemSet = AcquireLockTable.keySet();
        Iterator<String> it = itemSet.iterator();
        while (it.hasNext()){
            String item = it.next();
            System.out.println(item+": ");
            System.out.print("Acquire: ");
            LinkedList<Operation> acquireList = AcquireLockTable.get(item);
            Operation op1;
            if(acquireList != null) {
                for (int i = 0; i < acquireList.size(); i++) {
                    op1 = acquireList.get(i);
                    if (op1.type == Operation.READ) {
                        System.out.print(op1.siteID + " " + "r(" + op1.item + "), ");
                    } else {
                        System.out.print(op1.siteID + " " + "w(" + op1.item + "," + op1.value + "), ");
                    }
                }
            }
            System.out.println();
            System.out.print("Wait: ");
            LinkedList<Operation> waitList = WaitLockTable.get(item);
            Operation op2;
            if(waitList != null) {
                for (int i = 0; i < waitList.size(); i++) {
                    op2 = waitList.get(i);
                    if (op2.type == Operation.READ) {
                        System.out.print(op2.siteID + " " + "r(" + op2.item + "), ");
                    } else {
                        System.out.print(op2.siteID + " " + "w(" + op2.item + "," + op2.value + "), ");
                    }
                }
            }
            System.out.println();
        }
    }

    public boolean bfsToFindCycle(int n, int [][] edges) {
        int[] states = new int[n];
        ArrayList[] graph = new ArrayList[n];

        for(int i = 0; i < n; i++){
            graph[i] = new ArrayList();
        }

        for(int[] edge : edges){
            graph[edge[0]].add(edge[1]);
            graph[edge[1]].add(edge[0]);
        }

        Queue<Integer> queue = new LinkedList<>();

        queue.offer(0);
        states[0] = 1;
        int count = 0;

        while(!queue.isEmpty()){
            int node = queue.poll();
            count ++;
            for(int i = 0; i < graph[node].size(); i++){
                int next = (int) graph[node].get(i);

                if(states[next] == 1) return false ;
                else if(states[next] == 0){
                    states[next] = 1;
                    queue.offer(next);
                }
            }
            states[node] = 2;
        }

        return count == n;
    }

    public void abort(int siteId){
        Set<String> itemSet = AcquireLockTable.keySet();
        Iterator<String> it = itemSet.iterator();
        while (it.hasNext()){
            String item = it.next();
            LinkedList<Operation> acquireList = AcquireLockTable.get(item);
            Operation op1;
            if(acquireList != null) {
                for (int i = 0; i < acquireList.size(); i++) {
                    op1 = acquireList.get(i);
                    if (op1.siteID == siteId) {
                        Operation remove = acquireList.remove(i);
                        for (int j = 0; j < edges.length; j++) {
                            if(edges[j][remove.siteID] == 1){
                                edges[j][remove.siteID] = 0;
                            }
                        }
                    }
                }
            }
            LinkedList<Operation> waitList = WaitLockTable.get(item);
            Operation op2;
            if(waitList != null) {
                for (int i = 0; i < waitList.size(); i++) {
                    op2 = waitList.get(i);
                    if (op2.siteID == siteId) {
                        Operation remove = waitList.remove(i);
                        for (int j = 0; j < edges.length; j++) {
                            if(edges[j][remove.siteID] == 1){
                                edges[j][remove.siteID] = 0;
                            }
                        }
                    }
                }
            }
            AcquireLockTable.put(item,acquireList);
            WaitLockTable.put(item,waitList);
        }
        transactions.remove(siteId);
    }

    public boolean isWaiting(int siteID){
        for (int i = 0; i < edges[siteID].length; i++) {
            if(edges[siteID][i] == 1){
                return true;
            }
        }
        return false;
    }

    public void unlock(Operation operation) throws SQLException, IOException {
        service.sendLMRply(operation);
    }

    public void releaseWaitRelation(Operation op1, Operation op2){
        edges[op1.siteID][op2.siteID] = 0;
    }
}
