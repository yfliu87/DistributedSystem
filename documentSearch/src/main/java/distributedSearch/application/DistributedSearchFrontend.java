package distributedSearch.application;

import distributedSearch.clusterManagement.ServiceRegistry;
import distributedSearch.network.WebServer;
import distributedSearch.search.frontend.UserSearchHandler;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

import static distributedSearch.Params.COORDINATORS_REGISTRY_ZNODE;
import static distributedSearch.Params.ZOOKEEPER_ADDRESS;
import static distributedSearch.Params.SESSION_TIMEOUT;

public class DistributedSearchFrontend implements Watcher {

    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        int currentServicePort = 9000;
        if (args.length == 1) {
            currentServicePort = Integer.parseInt(args[0]);
        }

        DistributedSearchFrontend distributedSearchFrontend = new DistributedSearchFrontend();

        ZooKeeper zooKeeper = distributedSearchFrontend.connectToZookeeper();

        ServiceRegistry coordinatorServiceRegistry = new ServiceRegistry(zooKeeper, COORDINATORS_REGISTRY_ZNODE);

        UserSearchHandler searchHandler = new UserSearchHandler(coordinatorServiceRegistry);

        WebServer webServer = new WebServer(currentServicePort, searchHandler);
        webServer.startServer();

        System.out.println("Search frontend server is listening on port " + currentServicePort);

        distributedSearchFrontend.run();
        distributedSearchFrontend.close();
        System.out.println("Search frontend disconnected from Zookeeper, exiting ...");
    }

    public ZooKeeper connectToZookeeper() throws IOException {
        return new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Frontend successfully connected to zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Frontend disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
        }
    }
}
