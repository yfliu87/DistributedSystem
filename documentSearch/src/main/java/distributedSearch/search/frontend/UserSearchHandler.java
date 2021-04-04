package distributedSearch.search.frontend;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.protobuf.InvalidProtocolBufferException;
import distributedSearch.clusterManagement.ServiceRegistry;
import distributedSearch.model.frontend.FrontendSearchRequest;
import distributedSearch.model.frontend.FrontendSearchResponse;
import distributedSearch.network.OnRequestCallback;
import distributedSearch.network.frontend.WebClient;
import model.proto.SearchModel;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static distributedSearch.utils.Params.DOCUMENT_SEARCH_ENDPOINT;

public class UserSearchHandler implements OnRequestCallback {
    private final WebClient client;
    private final ServiceRegistry searchCoordinatorRegistry;
    private final ObjectMapper objectMapper;

    public UserSearchHandler(ServiceRegistry searchCoordinatorRegistry) {
        this.searchCoordinatorRegistry = searchCoordinatorRegistry;
        this.client = new WebClient();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    }

    @Override
    public String getEndpoint() {
        return DOCUMENT_SEARCH_ENDPOINT;
    }

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        try {
            FrontendSearchRequest frontendSearchRequest =
                    objectMapper.readValue(requestPayload, FrontendSearchRequest.class);

            FrontendSearchResponse frontendSearchResponse = createFrontendResponse(frontendSearchRequest);

            return objectMapper.writeValueAsBytes(frontendSearchResponse);
        } catch (IOException e) {
            return new byte[0];
        }
    }

    private FrontendSearchResponse createFrontendResponse(FrontendSearchRequest frontendSearchRequest) {
        SearchModel.Response searchClusterResponse = sendRequestToSearchCluster(frontendSearchRequest.getSearchQuery());

        List<FrontendSearchResponse.SearchResultInfo> filteredResults =
                filterResults(searchClusterResponse,
                        frontendSearchRequest.getMaxNumberOfResults(),
                        frontendSearchRequest.getMinScore());

        return new FrontendSearchResponse(filteredResults);
    }

    private SearchModel.Response sendRequestToSearchCluster(String searchQuery) {
        SearchModel.Request searchRequest = SearchModel.Request.newBuilder()
                .setSearchQuery(searchQuery)
                .build();

        try {
            String coordinatorAddress = searchCoordinatorRegistry.getRandomServiceAddress();
            if (coordinatorAddress == null) {
                System.out.println("Search cluster coordinator is unavailable.");
                return SearchModel.Response.getDefaultInstance();
            }

            byte[] payloadBody = client.sendTask(coordinatorAddress, searchRequest.toByteArray()).join();
            return SearchModel.Response.parseFrom(payloadBody);
        } catch (InterruptedException | KeeperException | InvalidProtocolBufferException e) {
            e.printStackTrace();
            return SearchModel.Response.getDefaultInstance();
        }
    }

    private List<FrontendSearchResponse.SearchResultInfo> filterResults(SearchModel.Response searchClusterResponse,
                                                                        long maxResults,
                                                                        double minScore) {
        double maxScore = getMaxScore(searchClusterResponse);

        return IntStream.range(0, (int) Math.min(searchClusterResponse.getRelevantDocumentsCount(), maxResults))
                .mapToObj(idx -> new int[]{idx, normalizeScore(searchClusterResponse.getRelevantDocuments(idx).getScore(), maxScore)})
                .filter(scoreObj -> ((int[])scoreObj)[1] >= minScore)
                .map(scoreObj -> {
                    int score = ((int[])scoreObj)[1];
                    int idx = ((int[])scoreObj)[0];

                    String documentName = searchClusterResponse.getRelevantDocuments(idx).getDocumentName();
                    String title = getDocumentTitle(documentName);
                    String extension = getDocumentExtension(documentName);

                    return new FrontendSearchResponse.SearchResultInfo(title, extension, score);
                })
                .collect(Collectors.toList());
    }

    private static String getDocumentExtension(String document) {
        String[] parts = document.split("\\.");
        if (parts.length == 2) {
            return parts[1];
        }
        return "";
    }

    private static String getDocumentTitle(String document) {
        return document.split("\\.")[0];
    }

    private static int normalizeScore(double inputScore, double maxScore) {
        return (int) Math.ceil(inputScore * 100.0 / maxScore);
    }

    private static double getMaxScore(SearchModel.Response searchClusterResponse) {
        if (searchClusterResponse.getRelevantDocumentsCount() == 0) {
            return 0;
        }

        return searchClusterResponse.getRelevantDocumentsList()
                .stream()
                .map(document -> document.getScore())
                .max(Double::compareTo)
                .get();
    }
}
