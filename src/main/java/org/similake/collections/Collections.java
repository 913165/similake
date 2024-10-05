package org.similake.collections;

import org.similake.model.Distance;
import org.similake.model.Point;
import org.similake.model.VectorStore;

import java.util.HashMap;
import java.util.Map;

public class Collections {
    private Map<String, VectorStore> collectionMap;

    // Constructor to initialize collection map
    public Collections() {
        this.collectionMap = new HashMap<>();
    }

    // Create and add a new VectorStore (instead of Collection) with vector size and distance metric
    public void addVectorStore(String storeName, int size, Distance distanceType) {
        VectorStore vectorStore = new VectorStore(size, distanceType);
        collectionMap.put(storeName, vectorStore);
    }

    // Get all VectorStores
    public Map<String, VectorStore> getAllVectorStores() {
        return collectionMap;
    }

    // Get a specific VectorStore by name
    public VectorStore getVectorStoreByName(String storeName) {
        return collectionMap.get(storeName);
    }

    // Add a Point (vector) to a specific VectorStore
    public void addPointToVectorStore(String storeName, Point point) {
        VectorStore vectorStore = collectionMap.get(storeName);
        if (vectorStore != null) {
            vectorStore.addPoint(point);
        } else {
            System.out.println("VectorStore not found: " + storeName);
        }
    }

    // Display all VectorStores and their points
    public void displayAllVectorStores() {
        collectionMap.forEach((name, vectorStore) -> {
            System.out.println("VectorStore Name: " + name + ", Size: " + vectorStore.getSize() + ", Distance Type: " + vectorStore.getDistanceType());
            vectorStore.getPoints().forEach(point -> System.out.println(point));
        });
    }


}
