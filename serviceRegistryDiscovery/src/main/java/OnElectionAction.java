import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback{
    private final ServiceRegistry serviceRegistry;
    private final int port;

    public OnElectionAction(ServiceRegistry serviceRegistry, int port) {
        this.serviceRegistry = serviceRegistry;
        this.port = port;
    }

    @Override
    public void onElectedToBeLeader() {
        this.serviceRegistry.unregisterFromCluster();
        this.serviceRegistry.registerForUpdates();
    }

    @Override
    public void onWorker() {
        try {
            String currentServerAddress = String.format("http://%s:%d", InetAddress.getLocalHost().getCanonicalHostName(), port);
            this.serviceRegistry.registerToCluster(currentServerAddress);
        } catch (KeeperException | InterruptedException | UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
