package org.similake.controller;

import org.similake.collections.Collections;
import org.similake.collections.config.CollectionConfig;
import org.similake.creteria.FilterCriteria;
import org.similake.model.Distance;
import org.similake.model.Payload;
import org.similake.model.Point;
import org.similake.model.VectorStore;
import org.similake.persist.VectorStoreService;
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
    private VectorStoreService vectorStoreService;

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
            String response = vectorStoreService.persistVectorToStorage(storeName, config);
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
        vectorStoreService.fetchAllCollectionConfigs().forEach((name, config) -> {
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
        vectorStoreService.fetchAllCollectionConfigs().forEach((name, config) -> {
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
            CollectionConfig config = vectorStoreService.fetchVectorFromStorage(storeName);
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
        String vectorNoeAvailable = "Vector store not found: " + vectorName;
        if (vectorStore == null) {
            logger.info("VectorStore not found in memory, checking RocksDB: {}", vectorName);
            // Fetch collection config from RocksDB if available
            CollectionConfig collectionConfig = vectorStoreService.fetchVectorFromStorage(vectorName);
            if (collectionConfig != null) {
                // Persist the point to RocksDB (if applicable)
                vectorStoreService.addPayloadToVectorStore(vectorName, point);
                return new ResponseEntity<>("Payload added successfully to " + vectorName, HttpStatus.CREATED);
            }
            else{
                return new ResponseEntity<>(vectorNoeAvailable, HttpStatus.NOT_FOUND);
            }
        }
       // assert vectorStore != null;
        vectorStore.addPoint(point);
        logger.info("Payload added to VectorStore: {}", vectorStore);
        logger.info("Current size of points: {}", vectorStore.getPoints().size());
        return new ResponseEntity<>("Payload added successfully to " + vectorName, HttpStatus.CREATED);
    }

    @PostMapping("/{vectorName}/payloads")
    public ResponseEntity<String> addPayloads(
            @PathVariable("vectorName") String vectorName,
            @RequestBody List<Payload> payloads) {

        logger.info("Bulk payload request received for vector store: {}. Number of payloads: {}",
                vectorName, payloads.size());

        // Input validation
        if (payloads == null || payloads.isEmpty()) {
            return new ResponseEntity<>("No payloads provided", HttpStatus.BAD_REQUEST);
        }

        // First, try to retrieve the vector store from memory
        VectorStore vectorStore = collections.getVectorStoreByName(vectorName);
        int successCount = 0;
        List<String> failedIds = new ArrayList<>();

        try {
            // Convert all payloads to points first
            List<Point> points = payloads.stream()
                    .map(payload -> {
                        try {
                            return new Point(
                                    UUID.fromString(payload.getId()),
                                    payload.getContent(),
                                    payload.getEmbedding(),
                                    payload.getMetadata()
                            );
                        } catch (IllegalArgumentException e) {
                            failedIds.add(payload.getId());
                            logger.error("Failed to process payload with ID: {}", payload.getId(), e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());

            // If the vector store is not in memory, check RocksDB
            if (vectorStore == null) {
                logger.info("VectorStore not found in memory, checking RocksDB: {}", vectorName);
                // Fetch collection config from RocksDB if available
                CollectionConfig collectionConfig = vectorStoreService.fetchVectorFromStorage(vectorName);
                if (collectionConfig != null) {
                    // Persist all points to RocksDB
                    for (Point point : points) {
                        vectorStoreService.addPayloadToVectorStore(vectorName, point);
                        successCount++;
                    }
                } else {
                    return new ResponseEntity<>("Vector store not found: " + vectorName,
                            HttpStatus.NOT_FOUND);
                }
            } else {
                // Add all points to in-memory vector store
                for (Point point : points) {
                    vectorStore.addPoint(point);
                    successCount++;
                }
                logger.info("Payloads added to VectorStore: {}. Current size of points: {}",
                        vectorStore, vectorStore.getPoints().size());
            }

            // Prepare response message
            StringBuilder responseMessage = new StringBuilder()
                    .append("Successfully added ")
                    .append(successCount)
                    .append(" payloads to ")
                    .append(vectorName);

            if (!failedIds.isEmpty()) {
                responseMessage.append(". Failed to process ")
                        .append(failedIds.size())
                        .append(" payloads with IDs: ")
                        .append(String.join(", ", failedIds));
                return new ResponseEntity<>(responseMessage.toString(), HttpStatus.PARTIAL_CONTENT);
            }

            return new ResponseEntity<>(responseMessage.toString(), HttpStatus.CREATED);

        } catch (Exception e) {
            logger.error("Error processing bulk payload request", e);
            return new ResponseEntity<>("Error processing bulk payload request: " + e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
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
                List<Point> allPointsFromVectorStore = vectorStoreService.getAllPointsFromVectorStore(vectorName);
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




    // Me thod to fetch CollectionConfig from RocksDB
    @GetMapping("/{collectionName}/config")
    public ResponseEntity<Object> fetchCollectionConfig(
            @PathVariable("collectionName") String collectionName,
            @RequestHeader("api-key") String apiKey) {
        logger.info("fetchCollectionConfig called with collectionName: {}", collectionName);
        // For security, you can add API key validation here if needed

        // Fetch the collection configuration from RocksDB
        CollectionConfig config = vectorStoreService.fetchVectorFromStorage(collectionName);

        // Check if the collection exists
        if (config == null) {
            return new ResponseEntity<>("Collection not found: " + collectionName, HttpStatus.NOT_FOUND);
        }

        // Return the fetched configuration as a response
        return new ResponseEntity<>(config, HttpStatus.OK);
    }

    // **New DELETE method to remove vector and its configuration**
    @DeleteMapping("/{storeName}")
    public ResponseEntity<String> removeVector(
            @PathVariable("storeName") String storeName) {

        logger.info("Request received to remove vector store: {}", storeName);
        boolean isRemoved = vectorStoreService.removeVector(storeName);
        boolean isRemoved2 = collections.removeVectorStore(storeName);
        if (isRemoved) {
            return new ResponseEntity<>("Vector store and configuration removed successfully", HttpStatus.OK);
        } else {
            return new ResponseEntity<>("Vector store or configuration not found", HttpStatus.NOT_FOUND);
        }
    }

   /* Endpoint to calculate cosine similarity between a query vector and all vectors in a store.
     * Returns vectors sorted by similarity score in descending order.
     *
             * @param vectorName the name of the vector store to search in
     * @param payload the query payload containing the vector to compare against
     * @param limit optional parameter to limit the number of results (default: 10)
     * @param threshold optional parameter to filter results below a similarity threshold (default: 0.0)
     * @param metadata optional parameter for filtering payloads based on metadata
     * @return ResponseEntity containing list of PayloadSimilarity objects
     */
    @PostMapping("/{vectorName}/similarity")
    public ResponseEntity<List<PayloadSimilarity>> calculateCosineSimilarity(
            @PathVariable("vectorName") String vectorName,
            @RequestBody Payload payload,
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(defaultValue = "0.0") double threshold,
            @RequestParam MultiValueMap<String, String> metadata) {

        logger.info("Calculating cosine similarity for vector store: {} with metadata filters", vectorName);

        try {
            // Validate input payload
            if (payload == null || payload.getEmbedding() == null || payload.getEmbedding().length == 0) {
                logger.error("Invalid input payload for similarity calculation");
                return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
            }

            // Get query vector
            float[] queryVector = payload.getEmbedding();

            // Get all payloads with applied filters
            List<Payload> filteredPayloads = getAllPayloadsForSimilarity(vectorName, metadata);

            if (filteredPayloads.isEmpty()) {
                logger.warn("No payloads found in vector store {} with given filters", vectorName);
                return new ResponseEntity<>(List.of(), HttpStatus.OK);
            }

            // Calculate similarities for filtered payloads
            List<PayloadSimilarity> similarities = new ArrayList<>();
            for (Payload candidatePayload : filteredPayloads) {
                float[] vector = candidatePayload.getEmbedding();
                Double similarity = vectorStoreService.calculateCosineSimilarity(queryVector, vector);

                if (similarity != null && similarity >= threshold) {
                    PayloadSimilarity payloadSimilarity = new PayloadSimilarity(candidatePayload, similarity);
                    similarities.add(payloadSimilarity);
                }
            }

            // Sort by similarity in descending order
            similarities.sort((a, b) -> Double.compare(b.getSimilarity(), a.getSimilarity()));

            // Limit results
            List<PayloadSimilarity> limitedResults = similarities.stream()
                    .limit(limit)
                    .collect(Collectors.toList());

            logger.info("Found {} similar vectors above threshold {} in store {} after filtering",
                    limitedResults.size(), threshold, vectorName);

            return new ResponseEntity<>(limitedResults, HttpStatus.OK);

        } catch (Exception e) {
            logger.error("Error calculating cosine similarity: {}", e.getMessage(), e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        }


    public List<Payload> getAllPayloadsForSimilarity(
            @PathVariable("vectorName") String vectorName,
            @RequestParam MultiValueMap<String, String> metadata) {

        List<FilterCriteria> filters = convertToFilterCriteria(metadata);
        logger.info("Filters: {}", filters);

        VectorStore vectorStore = collections.getVectorStoreByName(vectorName);
        if (vectorStore == null) {
            Map<String, VectorStore> allVectorStores2 = getAllVectorStores2();
            if (allVectorStores2.containsKey(vectorName)) {
                List<Point> allPointsFromVectorStore = vectorStoreService.getAllPointsFromVectorStore(vectorName);
                return allPointsFromVectorStore.stream()
                        .map(point -> new Payload(point.getId().toString(), point.getMetadata(),
                                point.getContent(), List.of(), point.getVector()))
                        .filter(point -> Utils.filterPayload(point, filters))
                        .collect(Collectors.toList());
            }
        }

        assert vectorStore != null;
        return vectorStore.getPoints().stream()
                .map(point -> new Payload(point.getId().toString(), point.getMetadata(),
                        point.getContent(), List.of(), point.getVector()))
                .filter(point -> Utils.filterPayload(point, filters))
                .collect(Collectors.toList());
    }

}

class PayloadSimilarity {
    private final Payload payload;
    private final double similarity;

    public PayloadSimilarity(Payload payload, double similarity) {
        this.payload = payload;
        this.similarity = similarity;
    }

    public Payload getPayload() {
        return payload;
    }

    public double getSimilarity() {
        return similarity;
    }


}

