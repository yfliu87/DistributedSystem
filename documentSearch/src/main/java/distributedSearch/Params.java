package distributedSearch;

public class Params {
    public static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    public static final int SESSION_TIMEOUT = 3000;
    public static final String ELECTION_NAMESPACE = "/election";
    public static final String WORKERS_REGISTRY_ZNODE = "/workers_service_registry";
    public static final String COORDINATORS_REGISTRY_ZNODE = "/coordinators_service_registry";
    public static final String STATUS_ENDPOINT = "/status";
    public static final String TASK_ENDPOINT = "/task";
    public static final String SEARCH_ENDPOINT = "/search";
    public static final String DOCUMENT_SEARCH_ENDPOINT = "/documents_search";
    public static final String RESOURCES_DIRECTORY = "/Users/yifeiliu/Documents/DistributedSystem/DistributedSystem/documentSearch/src/main/resources/books";
}
