package clusterCoordination;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private ZooKeeper zookeeper;
    private String currentZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException,
        KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();
        leaderElection.run();
        leaderElection.close();

        System.out.println("Disconnected from zookeeper, existing application...");
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zookeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
        System.out.println("znode name " + this.currentZnodeName);
    }

    public void reelectLeader() throws KeeperException, InterruptedException {
        Stat predecessorStat = null;
        String predecessorZnodeName = "";

        while(predecessorStat == null) {
            List<String> children = zookeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader");
                return;
            } else {
                System.out.println("I am not the leader");

                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorZnodeName = children.get(predecessorIndex);
                predecessorStat = zookeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }

        System.out.println("Watching znode " + predecessorZnodeName);
        System.out.println();
    }

    public void connectToZookeeper() throws IOException {
        this.zookeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException {
        synchronized (zookeeper) {
            zookeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zookeeper.close();
    }

    @Override
    public void process(final WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to zookeeper");
                } else {
                    synchronized (zookeeper) {
                        System.out.println("Received disconnect from zookeeper event");
                        zookeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
                break;
        }
    }

}
