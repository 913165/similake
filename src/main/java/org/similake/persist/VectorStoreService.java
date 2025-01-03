package org.similake.persist;

import org.similake.collections.config.CollectionConfig;
import org.similake.model.Point;

import java.util.List;
import java.util.Map;

public interface VectorStoreService {

    // Method to persist a collection's configuration to storage
    String persistVectorToStorage(String collectionName, CollectionConfig config);

    // Method to persist CollectionConfig to storage
    boolean createConfig(String collectionName, CollectionConfig config);

    // Method to fetch a CollectionConfig from storage
    CollectionConfig fetchVectorFromStorage(String collectionName);

    // Method to fetch all CollectionConfigs stored in the system
    Map<String, CollectionConfig> fetchAllCollectionConfigs();

    // Method to add a Point payload to a VectorStore
    void addPayloadToVectorStore(String vectorName, Point point);

    // Method to fetch all Points from storage for a given vector store
    List<Point> getAllPointsFromVectorStore(String vectorName);

    // **New Method** to remove a vector (all its points) and its configuration
    boolean removeVector(String collectionName);

    public Double calculateCosineSimilarity(float[] vector1, float[] vector2);
}
