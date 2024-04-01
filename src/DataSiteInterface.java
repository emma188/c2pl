import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

public interface DataSiteInterface extends Remote {
    void sendGrantLock(Operation op) throws IOException, SQLException;
    String sendUpdateMsg(Operation op) throws RemoteException, SQLException;
}
