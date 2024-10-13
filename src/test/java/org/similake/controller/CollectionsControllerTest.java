package org.similake.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.similake.collections.Collections;
import org.similake.collections.config.CollectionConfig;
import org.similake.model.Distance;
import org.similake.persist.RocksDBService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class CollectionsControllerTest {

    @Mock
    private RocksDBService rocksDBService;

    @Mock
    private Collections collections;

    @InjectMocks
    private CollectionsController collectionsController;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCreateVectorStore_PersistTrue() {
        String storeName = "testStore";
        String apiKey = "testApiKey";
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("size", 10);
        requestBody.put("distance", "Cosine");
        requestBody.put("persist", "true");

        CollectionConfig config = CollectionConfig.fromMap(storeName, requestBody);
        when(rocksDBService.persistVectorToStorage(anyString(), any(CollectionConfig.class))).thenReturn("Persisted");

        ResponseEntity<String> response = collectionsController.createVectorStore(storeName, apiKey, requestBody);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("Persisted", response.getBody());
        verify(rocksDBService, times(1)).persistVectorToStorage(anyString(), any(CollectionConfig.class));
    }

    @Test
    public void testCreateVectorStore_PersistFalse() {
        String storeName = "testStore";
        String apiKey = "testApiKey";
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("size", 10);
        requestBody.put("distance", "Cosine");
        requestBody.put("persist", "false");

        ResponseEntity<String> response = collectionsController.createVectorStore(storeName, apiKey, requestBody);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(storeName + "VectorStore created successfully ", response.getBody());
    }

    @Test
    public void testCreateVectorStore_InvalidDistanceMetric() {
        String storeName = "testStore";
        String apiKey = "testApiKey";
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("size", 10);
        requestBody.put("distance", "INVALID_METRIC");
        requestBody.put("persist", "false");

        ResponseEntity<String> response = collectionsController.createVectorStore(storeName, apiKey, requestBody);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertEquals("Invalid distance metric", response.getBody());
        //verify(collections, never()).addVectorStore(anyString(), anyInt(), any(Distance.class));
    }
}