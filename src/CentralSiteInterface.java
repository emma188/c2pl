import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

public interface CentralSiteInterface extends Remote {
    //String printH(String str) throws RemoteException;
    void establishConnection(String dataURL,int siteID) throws RemoteException;
    LockMsg sendOperation(Operation op) throws IOException, SQLException;
    void sendReleaseOp(Operation op) throws IOException, SQLException;
    void sendCount(int count) throws RemoteException, SQLException;
    void sendLMRply(Operation operation) throws IOException, SQLException;;
}
