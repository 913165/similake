package org.similake.persist;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.similake.collections.config.CollectionConfig;
import org.similake.model.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

@Service
public class RocksDBService implements VectorStoreService{
    private static final Logger logger = LoggerFactory.getLogger(RocksDBService.class);

    @Value("${db.path}")
    private  String COLLECTIONS_DIR;

    @Value("${config.path}")
    private  String CONFIG_DIR;
    static {
        RocksDB.loadLibrary();
    }

    @Override
    // Method to persist CollectionConfig to disk
    public String persistVectorToStorage(String collectionName, CollectionConfig config) {
        logger.info("Persisting collection to disk: {}", collectionName);
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, COLLECTIONS_DIR + collectionName)) {
                // Serialize CollectionConfig object to byte array
                createConfig(collectionName, config);
                logger.info("Persisted collection to disk: {}", collectionName);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        return "Created and persisted collection successfully: " + collectionName;
    }

    // Method to persist CollectionConfig to disk
    public boolean createConfig(String collectionName, CollectionConfig config) {
        logger.info("Persisting config to disk: " + collectionName);
        boolean isSuccess = true;
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, CONFIG_DIR + collectionName)) {
                // Serialize CollectionConfig object to byte array
                byte[] serializedConfig = serializeCollectionConfig(config);
                // Store serialized CollectionConfig in RocksDB using collectionName as the key
                rocksDB.put(collectionName.getBytes(), serializedConfig);
                logger.info("Persisted config to disk: {}", collectionName);
            }
        } catch (RocksDBException | IOException e) {
            throw new RuntimeException(e);

        }
        isSuccess = true;
        logger.info("Persisted config to disk: {}", isSuccess);
        return isSuccess;
    }

    // Helper method to serialize CollectionConfig to byte array
    private byte[] serializeCollectionConfig(CollectionConfig config) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(config);
            out.flush();
            return bos.toByteArray();
        }
    }

    // Method to fetch CollectionConfig from RocksDB
    public CollectionConfig fetchVectorFromStorage(String collectionName) {
        logger.info("Fetching collection from disk: {}", collectionName);
        if (Files.exists(Paths.get(CONFIG_DIR + collectionName))) {
        try (final Options options = new Options().setCreateIfMissing(false)) { // Set to false since the collection must already exist
            try (final RocksDB rocksDB = RocksDB.openReadOnly(options, CONFIG_DIR + collectionName)) {
                // Retrieve the serialized CollectionConfig byte array using collectionName as the key
                byte[] serializedConfig = rocksDB.get(collectionName.getBytes());
                // If no collection is found, return null or handle as needed
                if (serializedConfig == null) {
                    logger.warn("Collection not found: {}", collectionName);
                    return null;
                }
                // Deserialize the byte array back to CollectionConfig object
                return deserializeCollectionConfig(serializedConfig);
            }
        } catch (RocksDBException | IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }}
        else
        {
            logger.warn("Collection not found: {}", collectionName);
            return null;
        }
    }

    // Method to fetch all CollectionConfigs from RocksDB stored in the "config" folder
    public Map<String, CollectionConfig> fetchAllCollectionConfigs() {
        Map<String, CollectionConfig> collectionConfigs = new HashMap<>();
        // Iterate over each vector store (directory) in the "config" folder
        File configDirectory = new File(CONFIG_DIR);
        File[] configDirs = configDirectory.listFiles(File::isDirectory);
        if (configDirs != null) {
            for (File configDir : configDirs) {
                String vectorName = configDir.getName();
                // Fetch and deserialize the CollectionConfig for this vector store
                CollectionConfig collectionConfig = fetchVectorFromStorage(vectorName);
                if (collectionConfig != null) {
                    collectionConfigs.put(vectorName, collectionConfig);
                    logger.info("Fetched config for vector store: {}", vectorName);
                }
            }
        }
        return collectionConfigs;
    }

    // Helper method to deserialize byte array to CollectionConfig
    private CollectionConfig deserializeCollectionConfig(byte[] serializedConfig) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedConfig);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (CollectionConfig) in.readObject();
        }
    }

    // Method to add a Payload (Point) to a VectorStore and persist to RocksDB
    public String addPayloadToVectorStore(String vectorName, Point point) {
        logger.info("Adding payload to VectorStore: {}", vectorName);

        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, COLLECTIONS_DIR + vectorName)) {

                // Serialize Point object (representing Payload) to byte array
                byte[] serializedPoint = serializePoint(point);

                // Use the Point's UUID as the key and store the serialized payload (Point) in RocksDB
                rocksDB.put(point.getId().toString().getBytes(), serializedPoint);
                logger.info("Payload added to VectorStore: {}", vectorName);

            }
        } catch (RocksDBException | IOException e) {
            throw new RuntimeException("Failed to add payload to VectorStore", e);
        }

        return "Payload added successfully to " + vectorName;
    }

    // Helper method to serialize Point object to byte array
    private byte[] serializePoint(Point point) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(point);
            out.flush();
            return bos.toByteArray();
        }
    }
    // Method to fetch all Points from RocksDB for a given vector store
    public List<Point> getAllPointsFromVectorStore(String vectorName) {
        List<Point> points = new ArrayList<>();

        try (final Options options = new Options().setCreateIfMissing(false)) {
            try (final RocksDB rocksDB = RocksDB.open(options, COLLECTIONS_DIR + vectorName)) {
                // Iterate through all the key-value pairs in RocksDB
                RocksIterator iterator = rocksDB.newIterator();
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    byte[] serializedPoint = iterator.value();
                    Point point = deserializePoint(serializedPoint);
                    points.add(point);
                }
            }
        } catch (RocksDBException | IOException | ClassNotFoundException e) {
            logger.error("Error while fetching points from RocksDB for vector store: " + vectorName, e);
        }

        return points;
    }

    public List<Point> getAllPointsFromVectorStoreWithFilter(String vectorName, Map<String, Object> metadata) {
        List<Point> points = new ArrayList<>();

        try (final Options options = new Options().setCreateIfMissing(false)) {
            try (final RocksDB rocksDB = RocksDB.open(options, COLLECTIONS_DIR + vectorName)) {
                // Iterate through all the key-value pairs in RocksDB
                RocksIterator iterator = rocksDB.newIterator();
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    byte[] serializedPoint = iterator.value();
                    Point point = deserializePoint(serializedPoint);

                    // Apply metadata filtering
                    if (matchesMetadata(point, metadata)) {
                        points.add(point);
                    }
                }
            }
        } catch (RocksDBException | IOException | ClassNotFoundException e) {
            logger.error("Error while fetching points from RocksDB for vector store: " + vectorName, e);
        }

        return points;
    }

    /**
     * Checks if a point matches all the metadata criteria
     * @param point The point to check
     * @param metadata The metadata criteria to match against
     * @return true if the point matches all metadata criteria, false otherwise
     */
    private boolean matchesMetadata(Point point, Map<String, Object> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return true; // No filtering needed
        }

        Map<String, Object> pointMetadata = point.getMetadata();
        if (pointMetadata == null) {
            return false; // Point has no metadata, but filter criteria exist
        }

        return metadata.entrySet().stream().allMatch(entry -> {
            String key = entry.getKey();
            Object filterValue = entry.getValue();
            Object pointValue = pointMetadata.get(key);

            if (pointValue == null) {
                return false; // Required metadata field doesn't exist in point
            }

            // Handle different types of values
            if (filterValue instanceof String) {
                return filterValue.toString().equals(pointValue.toString());
            } else if (filterValue instanceof Number) {
                // Convert both to double for number comparison
                double filterNum = ((Number) filterValue).doubleValue();
                double pointNum = ((Number) pointValue).doubleValue();
                return Double.compare(filterNum, pointNum) == 0;
            } else if (filterValue instanceof Boolean) {
                return filterValue.equals(pointValue);
            } else if (filterValue instanceof Collection) {
                // If the filter value is a collection, check if point value is in that collection
                return ((Collection<?>) filterValue).contains(pointValue);
            }

            // Default to direct equality comparison
            return filterValue.equals(pointValue);
        });
    }

    // Helper method to deserialize byte array back to Point object
    private Point deserializePoint(byte[] serializedPoint) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedPoint);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (Point) in.readObject();
        }
    }

    // **Updated Method** to remove a vector and its configuration recursively
    @Override
    public boolean removeVector(String collectionName) {
        logger.info("Removing vector and config for collection: {}", collectionName);
        boolean isSuccess = false;

        try {
            // Recursively delete the vector data (all points)
            Path vectorDirPath = Paths.get(COLLECTIONS_DIR + collectionName);
            Path configDirPath = Paths.get(CONFIG_DIR + collectionName);

            // Recursively delete vector directory and its contents
            if (Files.exists(vectorDirPath)) {
                deleteDirectoryRecursively(vectorDirPath);
            }

            // Recursively delete config directory and its contents
            if (Files.exists(configDirPath)) {
                deleteDirectoryRecursively(configDirPath);
            }

            isSuccess = true;
            logger.info("Successfully removed vector and config for collection: {}", collectionName);
        } catch (IOException e) {
            logger.error("Error while removing vector or config for collection: " + collectionName, e);
        }

        return isSuccess;
    }

    // Helper method to recursively delete a directory and its contents
    private void deleteDirectoryRecursively(Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file); // Delete file
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir); // Delete directory after all contents are deleted
                return FileVisitResult.CONTINUE;
            }
        });
    }

}
