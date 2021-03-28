import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class NodeWatcher implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String TARGET_ZNODE = "/target_znode";
    private ZooKeeper zookeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        NodeWatcher nodeWatcher = new NodeWatcher();
        nodeWatcher.connectToZookeeper();
        nodeWatcher.watchTargetZnode();
        nodeWatcher.run();
        nodeWatcher.close();

        System.out.println("Disconnected from zookeeper, existing application...");
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

    public void watchTargetZnode() throws KeeperException, InterruptedException {
        Stat stat = zookeeper.exists(TARGET_ZNODE, this);
        if (stat == null) {
            return;
        }

        byte[] data = zookeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zookeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("Data: " + new String(data) + " children: " + children);
    }

    @Override
    public void process(final WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to zookeeper");
                } else {
                    synchronized (zookeeper) {
                        System.out.println("Received disconnect from zookeeper event");
                        zookeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                System.out.println(TARGET_ZNODE + " was deleted");
                break;
            case NodeCreated:
                System.out.println(TARGET_ZNODE + " was created");
                break;
            case NodeDataChanged:
                System.out.println(TARGET_ZNODE + " data changed");
                break;
            case NodeChildrenChanged:
                System.out.println(TARGET_ZNODE + " children changed");
                break;
        }

        try {
            watchTargetZnode();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
