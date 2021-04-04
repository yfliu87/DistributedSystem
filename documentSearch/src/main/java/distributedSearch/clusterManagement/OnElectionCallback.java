package distributedSearch.clusterManagement;

public interface OnElectionCallback {

    void onElectedToBeLeader();

    void onWorker();
}
