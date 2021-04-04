package distributedSearch.clusterManagement;

import distributedSearch.network.backend.WebClient;
import distributedSearch.network.backend.WebServer;
import distributedSearch.search.backend.SearchCoordinator;
import distributedSearch.search.backend.SearchWorker;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {
    private final ServiceRegistry workersServiceRegistry;
    private final ServiceRegistry coordinatorsServiceRegistry;
    private final int port;
    private WebServer webServer;

    public OnElectionAction(ServiceRegistry workersServiceRegistry,
                            ServiceRegistry coordinatorsServiceRegistry,
                            int port) {
        this.workersServiceRegistry = workersServiceRegistry;
        this.coordinatorsServiceRegistry = coordinatorsServiceRegistry;
        this.port = port;
    }

    @Override
    public void onElectedToBeLeader() {
        workersServiceRegistry.unregisterFromCluster();
        workersServiceRegistry.registerForUpdates();

        if (webServer != null) {
            webServer.stop();
        }

        SearchCoordinator searchCoordinator = new SearchCoordinator(workersServiceRegistry, new WebClient());
        webServer = new WebServer(port, searchCoordinator);
        webServer.startServer();

        try {
            String currentServerAddress = String.format(
                    "http://%s:%d%s", InetAddress.getLocalHost().getCanonicalHostName(),
                    port, searchCoordinator.getEndpoint());

            coordinatorsServiceRegistry.registerToCluster(currentServerAddress);

        } catch (InterruptedException | UnknownHostException | KeeperException e) {
            e.printStackTrace();
            return;
        }
    }

    @Override
    public void onWorker() {
        SearchWorker searchWorker = new SearchWorker();
        webServer = new WebServer(port, searchWorker);
        webServer.startServer();

        try {
            String currentServerAddress = String.format(
                    "http://%s:%d%s", InetAddress.getLocalHost().getCanonicalHostName(),
                    port, searchWorker.getEndpoint());

            workersServiceRegistry.registerToCluster(currentServerAddress);
        } catch (InterruptedException | UnknownHostException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
