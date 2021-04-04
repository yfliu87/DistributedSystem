package distributedSearch.search.backend;

import com.google.protobuf.InvalidProtocolBufferException;
import distributedSearch.clusterManagement.ServiceRegistry;
import distributedSearch.model.backend.DocumentData;
import distributedSearch.model.backend.Result;
import distributedSearch.model.backend.SerializationUtils;
import distributedSearch.model.backend.Task;
import distributedSearch.network.OnRequestCallback;
import distributedSearch.network.backend.WebClient;
import model.proto.SearchModel;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static distributedSearch.Params.RESOURCES_DIRECTORY;
import static distributedSearch.Params.SEARCH_ENDPOINT;

public class SearchCoordinator implements OnRequestCallback {

    private final ServiceRegistry workersServiceRegistry;
    private final WebClient client;
    private final List<String> documents;

    public SearchCoordinator(ServiceRegistry workersServiceRegistry, WebClient client) {
        this.workersServiceRegistry = workersServiceRegistry;
        this.client = client;
        this.documents = readDocumentsList();
    }

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        try {
            SearchModel.Request request = SearchModel.Request.parseFrom(requestPayload);
            SearchModel.Response response = createResponse(request);

            return response.toByteArray();
        } catch (InvalidProtocolBufferException | KeeperException | InterruptedException e) {
            e.printStackTrace();
            return SearchModel.Response.getDefaultInstance().toByteArray();
        }
    }

    @Override
    public String getEndpoint() {
        return SEARCH_ENDPOINT;
    }

    private SearchModel.Response createResponse(SearchModel.Request searchRequest)
            throws KeeperException, InterruptedException {

        SearchModel.Response.Builder searchResponse = SearchModel.Response.newBuilder();

        List<String> searchTerms = TFIDF.getWordsFromLine(searchRequest.getSearchQuery());
        List<String> workers = workersServiceRegistry.getAllServiceAddresses();

        if (workers.isEmpty()) {
            System.out.println("No search workers currently available");
            return searchResponse.build();
        }

        List<Task> tasks = createTasks(workers.size(), searchTerms);
        List<Result> results = sendTasksToWorkers(workers, tasks);

        List<SearchModel.Response.DocumentStats> sortedDocuments = aggregateResults(results, searchTerms);
        searchResponse.addAllRelevantDocuments(sortedDocuments);

        return searchResponse.build();
    }

    private List<SearchModel.Response.DocumentStats> aggregateResults(List<Result> results, List<String> searchTerms) {
        Map<String, DocumentData> allDocumentsResults = results.stream()
                .flatMap(result -> result.getDocumentToDocumentData().entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        System.out.println("Calculating score for all the docs");
        Map<Double, List<String>> scoreToDocuments = TFIDF.getDocumentsSortedByScore(searchTerms, allDocumentsResults);
        return convertDocumentsSortedByScore(scoreToDocuments);
    }

    private List<SearchModel.Response.DocumentStats> convertDocumentsSortedByScore(Map<Double, List<String>> scoreToDocuments) {

        return scoreToDocuments.entrySet().stream()
                .flatMap(entry -> {
                            return entry.getValue().stream()
                                    .map(document -> {
                                        File documentPath = new File(document);

                                        return SearchModel.Response.DocumentStats.newBuilder()
                                                .setScore(entry.getKey())
                                                .setDocumentName(documentPath.getName())
                                                .setDocumentSize(documentPath.length())
                                                .build();
                                    })
                                    .collect(Collectors.toList()).stream();
                        })
                    .collect(Collectors.toList());
    }

    private List<Result> sendTasksToWorkers(List<String> workers, List<Task> tasks) {
        CompletableFuture<Result>[] futures = new CompletableFuture[workers.size()];

        IntStream.range(0, workers.size())
                .forEach(idx -> {
                    String worker = workers.get(idx);
                    Task task = tasks.get(idx);
                    byte[] payload = SerializationUtils.serialize(task);
                    futures[idx] = client.sendTask(worker, payload);
                });

        return Stream.of(futures).map(CompletableFuture::join).collect(Collectors.toList());
    }

    public List<Task> createTasks(int numberOfWorkers, List<String> searchTerms) {
        return splitDocumentList(numberOfWorkers, documents)
                .stream().map(docs -> new Task(searchTerms, docs))
                .collect(Collectors.toList());
    }

    private static List<List<String>> splitDocumentList(int numberOfWorkers, List<String> documents) {
        int numberOfDocumentsPerWorker = (documents.size() + numberOfWorkers - 1) / numberOfWorkers;

        return IntStream.range(0, numberOfWorkers)
                .mapToObj(idx -> {
                    int firstDocumentIndex = idx * numberOfDocumentsPerWorker;
                    int lastDocumentIndexExclusive = Math.min(firstDocumentIndex + numberOfDocumentsPerWorker, documents.size());

                    return new ArrayList<String>(documents.subList(firstDocumentIndex, lastDocumentIndexExclusive));
                })
                .collect(Collectors.toList());
    }

    private static List<String> readDocumentsList() {
        File documentsDirectory = new File(RESOURCES_DIRECTORY);
        return Arrays.asList(documentsDirectory.list())
                .stream()
                .map(documentName -> RESOURCES_DIRECTORY + "/" + documentName)
                .collect(Collectors.toList());
    }
}
