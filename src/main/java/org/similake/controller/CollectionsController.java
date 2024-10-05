package org.similake.controller;

import org.similake.collections.Collections;
import org.similake.model.Distance;
import org.similake.model.Payload;
import org.similake.model.Point;
import org.similake.model.VectorStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/collections")
public class CollectionsController {

    private final Collections collections;

    private static final Logger logger = LoggerFactory.getLogger(CollectionsController.class);

    public CollectionsController() {
        this.collections = new Collections();  // Initialize collections
    }

    @PutMapping("/{storeName}")
    public ResponseEntity<String> createVectorStore(
            @PathVariable("storeName") String storeName,
            @RequestHeader("api-key") String apiKey,
            @RequestBody Map<String, Object> requestBody) {

        int size = (int) requestBody.get("size");
        String distanceMetric = (String) requestBody.get("distance");
        Distance distanceType;

        // Convert distance metric to enum
        try {
            distanceType = Distance.valueOf(distanceMetric);
        } catch (IllegalArgumentException e) {
            return new ResponseEntity<>("Invalid distance metric", HttpStatus.BAD_REQUEST);
        }

        // Create the VectorStore with the extracted parameters
        collections.addVectorStore(storeName, size, distanceType);
        collections.displayAllVectorStores();

        return new ResponseEntity<>("VectorStore created successfully", HttpStatus.OK);
    }



    // GET endpoint to retrieve all VectorStores
    @GetMapping
    public ResponseEntity<Map<String, VectorStore>> getAllVectorStores() {
        return new ResponseEntity<>(collections.getAllVectorStores(), HttpStatus.OK);
    }

    // GET endpoint to retrieve a specific VectorStore by name
    @GetMapping("/{storeName}")
    public ResponseEntity<VectorStore> getVectorStore(
            @PathVariable("storeName") String storeName) {

        VectorStore vectorStore = collections.getVectorStoreByName(storeName);
        if (vectorStore != null) {
            return new ResponseEntity<>(vectorStore, HttpStatus.OK);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @PostMapping("/{vectorName}/payload")
    public ResponseEntity<String> addPayload(@PathVariable("vectorName") String vectorName, @RequestBody Payload payload) {
        logger.info("Payload received: {}", payload);

        VectorStore vectorStore = collections.getVectorStoreByName(vectorName);
        if (vectorStore == null) {
            return new ResponseEntity<>("VectorStore not found: " + vectorName, HttpStatus.NOT_FOUND);
        }
        // Create a Point object from the Payload
        UUID id = UUID.fromString(payload.getId()); // Generate a new UUID for the Point
        String content = payload.getContent(); // Assuming Payload has a getContent() method'
        float[] embedding = payload.getEmbedding(); // Assuming Payload has a getEmbedding() method
        Point point = new Point(id, content, embedding);
        vectorStore.addPoint(point);
        logger.info("Payload added to VectorStore: {}", vectorStore);
        logger.info("size of points: {}", vectorStore.getPoints().size());

       // logger.info(" collections.getAllVectorStores() : {}",  collections.getAllVectorStores());
        return new ResponseEntity<>("Payload added successfully to " + vectorName, HttpStatus.CREATED);
    }

    @GetMapping("/{vectorName}/payloads")
    public ResponseEntity<List<Payload>> getAllPayloads(@PathVariable("vectorName") String vectorName) {
        VectorStore vectorStore = collections.getVectorStoreByName(vectorName);
        if (vectorStore == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        List<Payload> payloads = vectorStore.getPoints().stream()
                .map(point -> new Payload(point.getId().toString(), Map.of("content", point.getContent()), point.getContent(), List.of(), point.getVector()))
                .collect(Collectors.toList());
        return new ResponseEntity<>(payloads, HttpStatus.OK);
    }
}