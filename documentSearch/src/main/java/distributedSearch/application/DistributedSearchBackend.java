package distributedSearch.application;

import distributedSearch.clusterManagement.LeaderElection;
import distributedSearch.clusterManagement.OnElectionAction;
import distributedSearch.clusterManagement.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

import static distributedSearch.Params.WORKERS_REGISTRY_ZNODE;
import static distributedSearch.Params.COORDINATORS_REGISTRY_ZNODE;
import static distributedSearch.Params.ZOOKEEPER_ADDRESS;
import static distributedSearch.Params.SESSION_TIMEOUT;

public class DistributedSearchBackend implements Watcher {

    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        int currentServicePort = 8080;
        if (args.length == 1) {
            currentServicePort = Integer.parseInt(args[0]);
        }

        DistributedSearchBackend distributedSearchBackend = new DistributedSearchBackend();
        ZooKeeper zooKeeper = distributedSearchBackend.connectToZookeeper();

        ServiceRegistry workersServiceRegistry = new ServiceRegistry(zooKeeper, WORKERS_REGISTRY_ZNODE);
        ServiceRegistry coordinatorsServiceRegistry = new ServiceRegistry(zooKeeper, COORDINATORS_REGISTRY_ZNODE);

        OnElectionAction onElectionAction = new OnElectionAction(workersServiceRegistry, coordinatorsServiceRegistry, currentServicePort);

        LeaderElection leaderElection = new LeaderElection(zooKeeper, onElectionAction);
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();

        distributedSearchBackend.run();
        distributedSearchBackend.close();
        System.out.println("Search backend disconnected from zookeeper, exiting...");
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
                    System.out.println("Backend successfully connected to zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Backend disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
        }

    }

}
