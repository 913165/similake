package org.similake.collections.config;

import org.similake.model.Distance;

import java.util.Map;

public class CollectionConfig implements java.io.Serializable {
    private String collectionName;
    private int size;
    private Distance distance;
    private boolean persist;

    // Constructor
    public CollectionConfig(String collectionName, int size, Distance distance, boolean persist) {
        this.collectionName = collectionName;
        this.size = size;
        this.distance = distance;
        this.persist = persist;
    }

    // Getters and setters
    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Distance getDistance() {
        return distance;
    }

    public void setDistance(Distance distance) {
        this.distance = distance;
    }

    public boolean isPersist() {
        return persist;
    }

    public void setPersist(boolean persist) {
        this.persist = persist;
    }

    @Override
    public String toString() {
        return "CollectionConfig{" +
                "collectionName='" + collectionName + '\'' +
                ", size=" + size +
                ", distance=" + distance +
                ", persist=" + persist +
                '}';
    }

    // Method to create CollectionConfig from JSON-like Map input
    public static CollectionConfig fromMap(String VectorName,Map<String, Object> requestBody) {
        int size = (int) requestBody.get("size");
        String distanceMetric = (String) requestBody.get("distance");
        boolean persist = Boolean.parseBoolean((String) requestBody.get("persist"));

        Distance distanceType;
        try {
            distanceType = Distance.valueOf(distanceMetric);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Invalid distance metric: " + distanceMetric);
        }
        return new CollectionConfig(VectorName, size, distanceType, persist);
    }
}
