package distributedSearch.clusterManagement;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ServiceRegistry implements Watcher {

    private final ZooKeeper zooKeeper;
    private List<String> allServiceAddresses = null;
    private String currentZnode = null;
    private final String serviceRegistryZnode;

    public ServiceRegistry(ZooKeeper zooKeeper, String serviceRegistryZnode) {
        this.zooKeeper = zooKeeper;
        this.serviceRegistryZnode = serviceRegistryZnode;
        createServiceRegistryNode();
    }

    public void registerToCluster(String metadata) throws KeeperException, InterruptedException {
        if (currentZnode != null) {
            System.out.println("Already registered to service registry");
            return;
        }

        this.currentZnode = zooKeeper.create(serviceRegistryZnode + "/n_",
                metadata.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("Registered to service registry");
    }

    public void registerForUpdates() {
        try {
            updateAddresses();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void unregisterFromCluster() {
        try {
            if (currentZnode != null && zooKeeper.exists(currentZnode, false) != null) {
                zooKeeper.delete(currentZnode, -1);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws KeeperException, InterruptedException {
        if (allServiceAddresses == null) {
            updateAddresses();
        }
        return allServiceAddresses;
    }

    public synchronized String getRandomServiceAddress() throws KeeperException, InterruptedException {
        if (allServiceAddresses == null) {
            updateAddresses();
        }

        if (!allServiceAddresses.isEmpty()) {
            int randomIndex = new Random().nextInt(allServiceAddresses.size());
            return allServiceAddresses.get(randomIndex);
        } else {
            return null;
        }
    }

    private synchronized void updateAddresses() throws KeeperException, InterruptedException {
        List<String> workers = zooKeeper.getChildren(serviceRegistryZnode, this);
        List<String> addresses = new ArrayList<>(workers.size());

        for (String worker : workers) {
            String serviceFullPath = serviceRegistryZnode + "/" + worker;
            Stat stat = zooKeeper.exists(serviceFullPath, false);

            if (stat == null) {
                continue;
            }

            String address = new String(zooKeeper.getData(serviceFullPath, false, stat));
            addresses.add(address);
        }

        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The cluster addresses are: " + this.allServiceAddresses);
    }

    private void createServiceRegistryNode() {
        try {
            if (zooKeeper.exists(serviceRegistryZnode, false) == null) {
                zooKeeper.create(serviceRegistryZnode, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            updateAddresses();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
