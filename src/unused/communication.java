package unused;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface communication extends Remote {
    String printHello(int id) throws RemoteException;
    //String SendTransaction(String transaction) throws R
    String establishConnection(String dataURL,int siteID) throws RemoteException;
    String printH(String str) throws RemoteException;
    //String sendAck(String str)
}
