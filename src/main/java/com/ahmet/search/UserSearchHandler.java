package com.ahmet.search;

import com.ahmet.cluster.management.ServiceRegistry;
import com.ahmet.model.frontend.FrontendSearchRequest;
import com.ahmet.model.frontend.FrontendSearchResponse;
import com.ahmet.networking.CoordinatorClient;
import com.ahmet.networking.OnRequestCallback;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class UserSearchHandler implements OnRequestCallback {
    private final Logger logger;
    private static final String ENDPOINT = "/documents_search";
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
        return null;
    }

    @Override
    public String getEndpoint() {
        return ENDPOINT;
    }
}
