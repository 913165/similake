package org.similake.controller;

import org.similake.collections.config.CollectionConfig;
import org.similake.collections.Collections;
import org.similake.creteria.FilterCriteria;
import org.similake.model.Distance;
import org.similake.model.Payload;
import org.similake.model.Point;
import org.similake.model.VectorStore;
import org.similake.persist.RocksDBService;
import org.similake.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/collections")
public class CollectionsController {


    private final Collections collections;

    private static final Logger logger = LoggerFactory.getLogger(CollectionsController.class);

    public CollectionsController() {
        this.collections = new Collections();  // Initialize collections
    }

    @Autowired
    private RocksDBService rocksDBService;

    /**
     * Endpoint to create a new VectorStore.
     *
     * @param storeName   the name of the vector store to be created
     * @param apiKey      the API key for authentication
     * @param requestBody the request body containing the vector store configuration
     * @return a ResponseEntity with a success message or an error message
     */
    @PutMapping("/{storeName}")
    public ResponseEntity<String> createVectorStore(
            @PathVariable("storeName") String storeName,
            @RequestHeader("api-key") String apiKey,
            @RequestBody Map<String, Object> requestBody) {


        int size = (int) requestBody.get("size");
        String distanceMetric = (String) requestBody.get("distance");
        String persist = (String) requestBody.get("persist");
        Distance distanceType;

        // Convert distance metric to enum
        try {
            distanceType = Distance.valueOf(distanceMetric);
        } catch (IllegalArgumentException e) {
            return new ResponseEntity<>("Invalid distance metric", HttpStatus.BAD_REQUEST);
        }
        CollectionConfig config = CollectionConfig.fromMap(storeName, requestBody);
        // Conditionally persist or store in memory based on the `persistent` flag
        if (persist != null && persist.equals("true")) {
            // Code for persisting the vector store (e.g., save to disk or database)
            String response = rocksDBService.persistVectorToStorage(storeName, config);
            return new ResponseEntity<>(response, HttpStatus.OK);
        } else {
            // Create a new VectorStore and add it to the collections
            collections.addVectorStore(storeName, size, distanceType);
        }

        // Display all vector stores
        // collections.displayAllVectorStores();

        return new ResponseEntity<>(storeName + "VectorStore created successfully ", HttpStatus.OK);
    }


    /**
     * GET endpoint to retrieve all VectorStores.
     *
     * This method retrieves all vector stores from both in-memory and RocksDB storage.
     * It first fetches the in-memory vector stores and then combines them with the vector
     * stores fetched from RocksDB, ensuring no duplicates.
     *
     * @return a ResponseEntity containing a map of all vector stores and an HTTP status code
     */
    @GetMapping
    public ResponseEntity<Map<String, VectorStore>> getAllVectorStores() {
        // Retrieve in-memory vector stores
        Map<String, VectorStore> inMemoryStores = collections.getAllVectorStores();
        // Initialize a map to combine both in-memory and RocksDB vector stores
        Map<String, VectorStore> allVectorStores = new HashMap<>(inMemoryStores);
        // Fetch vector configurations from RocksDB
        rocksDBService.fetchAllCollectionConfigs().forEach((name, config) -> {
            // Only add the vector store from RocksDB if it's not already in memory
            if (!allVectorStores.containsKey(name)) {
                VectorStore vectorStore = new VectorStore(config.getSize(), config.getDistance());
                allVectorStores.put(name, vectorStore);
            }
        });

        // Return the combined result of both in-memory and RocksDB vector stores
        return new ResponseEntity<>(allVectorStores, HttpStatus.OK);
    }

    public Map<String, VectorStore> getAllVectorStores2() {
        // Retrieve in-memory vector stores
        Map<String, VectorStore> inMemoryStores = collections.getAllVectorStores();

        // Initialize a map to combine both in-memory and RocksDB vector stores
        Map<String, VectorStore> allVectorStores = new HashMap<>(inMemoryStores);

        // Fetch vector configurations from RocksDB
        rocksDBService.fetchAllCollectionConfigs().forEach((name, config) -> {
            // Only add the vector store from RocksDB if it's not already in memory
            if (!allVectorStores.containsKey(name)) {
                VectorStore vectorStore = new VectorStore(config.getSize(), config.getDistance());
                allVectorStores.put(name, vectorStore);
            }
        });
        // Return the combined result of both in-memory and RocksDB vector stores
        return allVectorStores;
    }


    // GET endpoint to retrieve a specific VectorStore by name
    @GetMapping("/{storeName}")
    public ResponseEntity<VectorStore> getVectorStore(
            @PathVariable("storeName") String storeName) {

        VectorStore vectorStore = collections.getVectorStoreByName(storeName);

        if (vectorStore == null) {
            // Fetch the collection configuration from RocksDB
            CollectionConfig config = rocksDBService.fetchVectorFromStorage(storeName);
            if (config == null) {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            } else {
                vectorStore = new VectorStore(config.getSize(), config.getDistance());
                return new ResponseEntity<>(vectorStore, HttpStatus.OK);
            }
        }
        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    @PostMapping("/{vectorName}/payload")
    public ResponseEntity<String> addPayload(@PathVariable("vectorName") String vectorName, @RequestBody Payload payload) {
        logger.info("Payload received: {}", payload);
        // Create a Point object from the Payload
        UUID id = UUID.fromString(payload.getId()); // Assuming Payload has a getId() method
        String content = payload.getContent();      // Assuming Payload has a getContent() method
        float[] embedding = payload.getEmbedding(); // Assuming Payload has a getEmbedding() method
        Map<String, Object> metadata = payload.getMetadata();// Assuming Payload has a getMetadata() method
        Point point = new Point(id, content, embedding, metadata);
        // First, try to retrieve the vector store from memory
        VectorStore vectorStore = collections.getVectorStoreByName(vectorName);
        // If the vector store is not in memory, check RocksDB
        if (vectorStore == null) {
            logger.info("VectorStore not found in memory, checking RocksDB: {}", vectorName);
            // Fetch collection config from RocksDB if available
            CollectionConfig collectionConfig = rocksDBService.fetchVectorFromStorage(vectorName);
            if (collectionConfig != null) {
                // Persist the point to RocksDB (if applicable)
                rocksDBService.addPayloadToVectorStore(vectorName, point);
                return new ResponseEntity<>("Payload added successfully to " + vectorName, HttpStatus.CREATED);
            }
        }
        assert vectorStore != null;
        vectorStore.addPoint(point);
        logger.info("Payload added to VectorStore: {}", vectorStore);
        logger.info("Current size of points: {}", vectorStore.getPoints().size());
        return new ResponseEntity<>("Payload added successfully to " + vectorName, HttpStatus.CREATED);
    }


    @GetMapping("/{vectorName}/payloads")
    public ResponseEntity<List<Payload>> getAllPayloads(
            @PathVariable("vectorName") String vectorName,
            @RequestParam MultiValueMap<String, String> metadata) {

        List<FilterCriteria> filters = convertToFilterCriteria(metadata);
        logger.info("Filters: {}", filters);

        VectorStore vectorStore = collections.getVectorStoreByName(vectorName);
        if (vectorStore == null) {
            Map<String, VectorStore> allVectorStores2 = getAllVectorStores2();
            if (allVectorStores2.containsKey(vectorName)) {
                List<Point> allPointsFromVectorStore = rocksDBService.getAllPointsFromVectorStore(vectorName);
                List<Payload> payloads = allPointsFromVectorStore.stream()
                        .map(point -> new Payload(point.getId().toString(), point.getMetadata(),
                                point.getContent(), List.of(), point.getVector()))
                        .filter(point -> Utils.filterPayload(point, filters))
                        .collect(Collectors.toList());
                return new ResponseEntity<>(payloads, HttpStatus.OK);
            }
        }

        assert vectorStore != null;
        List<Payload> payloads = vectorStore.getPoints().stream()
                .map(point -> new Payload(point.getId().toString(), point.getMetadata(),
                        point.getContent(), List.of(), point.getVector()))
                .filter(point -> Utils.filterPayload(point, filters))
                .collect(Collectors.toList());
        return new ResponseEntity<>(payloads, HttpStatus.OK);
    }

    private List<FilterCriteria> convertToFilterCriteria(MultiValueMap<String, String> metadata) {
        List<FilterCriteria> filters = new ArrayList<>();

        metadata.forEach((key, values) -> {
            if (key.startsWith("metadata.")) {
                String[] parts = key.split("\\.");
                if (parts.length >= 3) {  // metadata.field.operator format
                    String field = parts[1];
                    String operator = parts[2];
                    values.forEach(value -> {
                        FilterCriteria filter = new FilterCriteria();
                        filter.setField(field);
                        filter.setOperator(operator);
                        filter.setValue(Utils.parseValue(value));
                        filters.add(filter);
                    });
                }
            }
        });

        return filters;
    }




    // Method to fetch CollectionConfig from RocksDB
    @GetMapping("/{collectionName}/config")
    public ResponseEntity<Object> fetchCollectionConfig(
            @PathVariable("collectionName") String collectionName,
            @RequestHeader("api-key") String apiKey) {
        logger.info("fetchCollectionConfig called with collectionName: {}", collectionName);
        // For security, you can add API key validation here if needed

        // Fetch the collection configuration from RocksDB
        CollectionConfig config = rocksDBService.fetchVectorFromStorage(collectionName);

        // Check if the collection exists
        if (config == null) {
            return new ResponseEntity<>("Collection not found: " + collectionName, HttpStatus.NOT_FOUND);
        }

        // Return the fetched configuration as a response
        return new ResponseEntity<>(config, HttpStatus.OK);
    }

    @PostMapping("/{vectorName}/payload2")
    public ResponseEntity<String> addPayloadtodisk(@PathVariable("vectorName") String vectorName, @RequestBody Payload payload) {
        logger.info("Payload received: {}", payload);

        // Assuming you have a method to convert Payload to Point
        UUID id = UUID.fromString(payload.getId()); // Assuming Payload has getId() method
        String content = payload.getContent();      // Assuming Payload has getContent() method
        float[] embedding = payload.getEmbedding(); // Assuming Payload has getEmbedding() method
        Map<String, Object> metadata = payload.getMetadata();
        Point point = new Point(id, content, embedding, metadata);

        // Persist the point using RocksDBService
        String responseMessage = rocksDBService.addPayloadToVectorStore(vectorName, point);

        return new ResponseEntity<>(responseMessage, HttpStatus.CREATED);
    }

    @GetMapping("/{vectorName}/payloads2")
    public ResponseEntity<List<Payload>> getAllPayloadsFromDisk(@PathVariable("vectorName") String vectorName) {
        // First, check if the vector store exists in memory
        VectorStore vectorStore = collections.getVectorStoreByName(vectorName);

        List<Point> points;

        if (vectorStore != null) {
            // If the vector store exists in memory, get the points from it
            points = vectorStore.getPoints();
            logger.info("Fetched payloads from in-memory VectorStore: {}", vectorName);
        } else {
            // If not found in memory, fetch the data from RocksDB
            points = rocksDBService.getAllPointsFromVectorStore(vectorName);

            if (points == null || points.isEmpty()) {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND); // If no points are found in RocksDB
            }

            logger.info("Fetched payloads from RocksDB VectorStore: {}", vectorName);
        }

        // Convert the points to payloads
        List<Payload> payloads = points.stream()
                .map(point -> new Payload(
                        point.getId().toString(),
                        Map.of("content", point.getContent()),
                        point.getContent(),
                        List.of(),
                        point.getVector()))
                .collect(Collectors.toList());

        return new ResponseEntity<>(payloads, HttpStatus.OK);
    }

    // **New DELETE method to remove vector and its configuration**
    @DeleteMapping("/{storeName}")
    public ResponseEntity<String> removeVector(
            @PathVariable("storeName") String storeName) {

        logger.info("Request received to remove vector store: {}", storeName);
        boolean isRemoved = rocksDBService.removeVector(storeName);
        boolean isRemoved2 = collections.removeVectorStore(storeName);
        if (isRemoved) {
            return new ResponseEntity<>("Vector store and configuration removed successfully", HttpStatus.OK);
        } else {
            return new ResponseEntity<>("Vector store or configuration not found", HttpStatus.NOT_FOUND);
        }
    }

}