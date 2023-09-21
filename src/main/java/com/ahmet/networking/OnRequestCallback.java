package com.ahmet.networking;

public interface OnRequestCallback {

    // Request will be forwarded to the coordinator server /search context path
    byte[] handleRequest(byte[] requestPayload);

    // Will point to address of frontend server /documents_search
    String getEndpoint();
}
