package dewilson.projects.lookup.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface LookUp extends Remote {

    boolean keyExists(String key) throws RemoteException;

    String getValue(String key) throws RemoteException;

}