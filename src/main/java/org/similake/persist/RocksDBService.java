package org.similake.persist;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.similake.collections.config.CollectionConfig;
import org.similake.model.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class RocksDBService {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBService.class);

    private static final String dbPath = "./collections/";

    private static final String configPath = "./config/";

    static {
        RocksDB.loadLibrary();
    }

    // Method to persist CollectionConfig to disk
    public String persistVectorToDisk(String collectionName, CollectionConfig config) {
        logger.info("Persisting collection to disk: {}", collectionName);
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, dbPath + collectionName)) {
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
    public void createConfig(String collectionName, CollectionConfig config) {
        logger.info("Persisting config to disk: " + collectionName);
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB rocksDB = RocksDB.open(options, configPath + collectionName)) {
                // Serialize CollectionConfig object to byte array
                byte[] serializedConfig = serializeCollectionConfig(config);
                // Store serialized CollectionConfig in RocksDB using collectionName as the key
                rocksDB.put(collectionName.getBytes(), serializedConfig);
                logger.info("Persisted config to disk: {}", collectionName);
            }
        } catch (RocksDBException | IOException e) {
            throw new RuntimeException(e);
        }
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
    public CollectionConfig fetchVectorFromDisk(String collectionName) {
        logger.info("Fetching collection from disk: {}", collectionName);
        if (Files.exists(Paths.get(configPath + collectionName))) {
        try (final Options options = new Options().setCreateIfMissing(false)) { // Set to false since the collection must already exist
            try (final RocksDB rocksDB = RocksDB.openReadOnly(options, configPath + collectionName)) {
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
        File configDirectory = new File(configPath);
        File[] configDirs = configDirectory.listFiles(File::isDirectory);
        if (configDirs != null) {
            for (File configDir : configDirs) {
                String vectorName = configDir.getName();
                // Fetch and deserialize the CollectionConfig for this vector store
                CollectionConfig collectionConfig = fetchVectorFromDisk(vectorName);
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
            try (final RocksDB rocksDB = RocksDB.open(options, dbPath + vectorName)) {

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
            try (final RocksDB rocksDB = RocksDB.open(options, dbPath + vectorName)) {
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

    // Helper method to deserialize byte array back to Point object
    private Point deserializePoint(byte[] serializedPoint) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serializedPoint);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (Point) in.readObject();
        }
    }

}
