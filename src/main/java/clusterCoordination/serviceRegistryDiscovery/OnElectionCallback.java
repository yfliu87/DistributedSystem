package clusterCoordination.serviceRegistryDiscovery;

public interface OnElectionCallback {

    void onElectedToBeLeader();

    void onWorker();
}
