package com.ahmet.search;

import com.ahmet.cluster.management.ServiceRegistry;
import com.ahmet.model.frontend.FrontendSearchRequest;
import com.ahmet.model.frontend.FrontendSearchResponse;
import com.ahmet.model.proto.SearchModel;
import com.ahmet.networking.CoordinatorClient;
import com.ahmet.networking.OnRequestCallback;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

public class UserSearchHandler implements OnRequestCallback {
    private final Logger logger;
    private static final String ENDPOINT = "/documents_search";
    private static final String DOCUMENTS_DIRECTORY = "books";
    private final CoordinatorClient coordinatorClient;
    private final ServiceRegistry searchCoordinatorRegistry;
    private final ObjectMapper objectMapper;

    public UserSearchHandler(ServiceRegistry searchCoordinatorRegistry) {
        this.searchCoordinatorRegistry = searchCoordinatorRegistry;
        this.coordinatorClient = new CoordinatorClient();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false); // To be able to iterate on UI and server code independently.
        this.objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        this.logger = LoggerFactory.getLogger(UserSearchHandler.class);
    }

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        try {
            FrontendSearchRequest frontendSearchRequest = objectMapper.readValue(requestPayload, FrontendSearchRequest.class);
            FrontendSearchResponse frontendSearchResponse = createFrontendResponse(frontendSearchRequest);
            return objectMapper.writeValueAsBytes(frontendSearchResponse);
        } catch (IOException e) {
            logger.error("Error parsing the request", e);
            return new byte[0];
        }
    }

    private FrontendSearchResponse createFrontendResponse(FrontendSearchRequest frontendSearchRequest) {
        SearchModel.Response coordinatorResponse = sendRequestToCoordinator(frontendSearchRequest.getSearchQuery());
        List<FrontendSearchResponse.SearchResultInfo> filteredResults =
                filterResults(coordinatorResponse,
                        frontendSearchRequest.getMaxNumberOfResults(),
                        frontendSearchRequest.getMinScore());

        return new FrontendSearchResponse(filteredResults, DOCUMENTS_DIRECTORY);
    }

    private SearchModel.Response sendRequestToCoordinator(String searchQuery) {
        SearchModel.Request searchRequest = SearchModel.Request.newBuilder()
                .setSearchQuery(searchQuery)
                .build();

        try {
            String coordinatorAddress = searchCoordinatorRegistry.getRandomServiceAddress();
            if (coordinatorAddress == null) {
                logger.error("Coordinator unavailable");
                return SearchModel.Response.getDefaultInstance();
            }
            byte[] searchResponseBytes = coordinatorClient.sendTask(coordinatorAddress, searchRequest.toByteArray()).join();
            return SearchModel.Response.parseFrom(searchResponseBytes);
        } catch (InvalidProtocolBufferException | CancellationException | CompletionException e) {
            logger.error("Error retrieving search response from coordinator", e);
            return SearchModel.Response.getDefaultInstance();
        }
    }

    private List<FrontendSearchResponse.SearchResultInfo> filterResults(SearchModel.Response coordinatorResponse,
                                                                        long maxNumberOfResults,
                                                                        double minScore) {
        double maxCore = getMaxScore(coordinatorResponse);
        List<FrontendSearchResponse.SearchResultInfo> searchResultInfoList = new ArrayList<>();
        for (int i = 0; i < coordinatorResponse.getRelevantDocumentsCount() && i < maxNumberOfResults; i++) {
            SearchModel.Response.DocumentStats currentDocument = coordinatorResponse.getRelevantDocuments(i);
            int normalizedDocumentScore = normalizeScore(currentDocument.getScore(), maxCore);
            if (normalizedDocumentScore < minScore) { // Documents are ordered. When the first document not satisfying
                // min score is met, break the loop.
                break;
            }

            String documentName = currentDocument.getDocumentName();
            String title = getDocumentTitle(documentName);
            String extension = getDocumentExtension(documentName);

            FrontendSearchResponse.SearchResultInfo resultInfo = new FrontendSearchResponse.SearchResultInfo(title, extension, normalizedDocumentScore);
            searchResultInfoList.add(resultInfo);
        }
        return searchResultInfoList;
    }

    private String getDocumentExtension(String documentName) {
        return documentName.split("\\.")[1];
    }

    private String getDocumentTitle(String documentName) {
        return documentName.split("\\.")[0];
    }

    private int normalizeScore(double score, double maxCore) {
        return (int) Math.ceil(score / maxCore * 100);
    }

    private double getMaxScore(SearchModel.Response coordinatorResponse) {
        return coordinatorResponse.getRelevantDocumentsList().stream()
                .map(SearchModel.Response.DocumentStats::getScore)
                .max(Double::compareTo)
                .orElse(Double.MAX_VALUE);
    }

    @Override
    public String getEndpoint() {
        return ENDPOINT;
    }
}
