package org.similake.jsonpersist;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.similake.collections.config.CollectionConfig;
import org.similake.model.Distance;
import org.similake.model.Point;
import org.similake.persist.VectorStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Primary
@Service
public class JsonlVectorStoreService implements VectorStoreService {
    private static final Logger logger = LoggerFactory.getLogger(JsonlVectorStoreService.class);
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${db.path}")
    private String COLLECTIONS_DIR;

    @Value("${config.path}")
    private String CONFIG_DIR;

    @PostConstruct
    public void init() {
        try {
            // Create base directories if they don't exist
            createDirectoryStructure();
        } catch (IOException e) {
            logger.error("Failed to initialize directory structure", e);
            throw new RuntimeException("Failed to initialize storage directories", e);
        }
    }

    private void createDirectoryStructure() throws IOException {
        // Create main directories
        Path collectionsPath = Paths.get(COLLECTIONS_DIR);
        Path configPath = Paths.get(CONFIG_DIR);

        // Create directories with all parent directories
        Files.createDirectories(collectionsPath);
        Files.createDirectories(configPath);

        logger.info("Created directory structure:");
        logger.info("Collections directory: {}", collectionsPath.toAbsolutePath());
        logger.info("Config directory: {}", configPath.toAbsolutePath());
    }

    private void ensureCollectionExists(String collectionName) throws IOException {
        // Create collection-specific directories
        Path collectionDir = Paths.get(COLLECTIONS_DIR, collectionName);
        Path configDir = Paths.get(CONFIG_DIR, collectionName);

        // Create directories
        Files.createDirectories(collectionDir);
        Files.createDirectories(configDir);

        logger.info("Created collection directories:");
        logger.info("Collection directory: {}", collectionDir);
        logger.info("Config directory: {}", configDir);

        // Create or verify vector file
        Path vectorFile = collectionDir.resolve("vectors.jsonl");
        if (!Files.exists(vectorFile)) {
            Files.createFile(vectorFile);
            logger.info("Created vector file: {}", vectorFile);
        }

        // Create or verify config file
        Path configFile = configDir.resolve("config.json");
        if (!Files.exists(configFile)) {
            Files.createFile(configFile);
            logger.info("Created config file: {}", configFile);
        }
    }

    @Override
    public String persistVectorToStorage(String collectionName, CollectionConfig config) {
        logger.info("Persisting collection to disk: {}", collectionName);
        try {
            // Create necessary directories and files
            ensureCollectionExists(collectionName);

            // Persist config
            createConfig(collectionName, config);
            logger.info("Persisted collection to disk: {}", collectionName);

        } catch (IOException e) {
            logger.error("Failed to create collection: {}", e.getMessage());
            throw new RuntimeException("Failed to create collection: " + collectionName, e);
        }
        return "Created and persisted collection successfully: " + collectionName;
    }

    public boolean createConfig(String collectionName, CollectionConfig config) {
        logger.info("Persisting config to disk: {}", collectionName);
        try {
            Path configDir = Paths.get(CONFIG_DIR, collectionName);
            Files.createDirectories(configDir);

            Path configFile = configDir.resolve("config.json");
            if (!Files.exists(configFile)) {
                Files.createFile(configFile);
                logger.info("Created config file: {}", configFile);
            }

            // Write config to file
            mapper.writeValue(configFile.toFile(), config);
            logger.info("Persisted config to disk successfully for collection: {}", collectionName);
            return true;
        } catch (IOException e) {
            logger.error("Failed to persist config: {}", e.getMessage());
            throw new RuntimeException("Failed to persist config", e);
        }
    }
    public CollectionConfig fetchVectorFromStorage(String collectionName) {
        logger.info("Fetching collection from disk: {}", collectionName);
        Path configFile = Paths.get(CONFIG_DIR, collectionName, "config.json");

        if (Files.exists(configFile)) {
            try {
                // Read the JSON content as a Map first
                Map<String, Object> configMap = mapper.readValue(configFile.toFile(),
                        new TypeReference<Map<String, Object>>() {});

                // Convert the distance string to Distance enum
                String distanceStr = (String) configMap.get("distance");
                Distance distance = Distance.valueOf(distanceStr);

                // Create CollectionConfig using the existing fromMap method
                Map<String, Object> requestBody = new HashMap<>();
                requestBody.put("size", configMap.get("size"));
                requestBody.put("distance", distanceStr);
                requestBody.put("persist", String.valueOf(configMap.get("persist")));

                return CollectionConfig.fromMap(collectionName, requestBody);

            } catch (IOException e) {
                logger.error("Failed to read config for {}: {}. Config content: {}",
                        collectionName, e.getMessage(),
                        readFileContent(configFile));
                throw new RuntimeException("Failed to read config: " + collectionName, e);
            } catch (IllegalArgumentException e) {
                logger.error("Invalid distance metric in config for {}: {}",
                        collectionName, e.getMessage());
                throw new RuntimeException("Invalid config for: " + collectionName, e);
            }
        } else {
            logger.warn("Collection config not found: {}", collectionName);
            return null;
        }
    }

    // Helper method to safely read file content for error logging
    private String readFileContent(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            return "Unable to read file content";
        }
    }

    public Map<String, CollectionConfig> fetchAllCollectionConfigs() {
        Map<String, CollectionConfig> collectionConfigs = new HashMap<>();
        Path configDir = Paths.get(CONFIG_DIR);

        try {
            if (!Files.exists(configDir)) {
                logger.info("Config directory does not exist. Creating it.");
                Files.createDirectories(configDir);
                return collectionConfigs;
            }

            // List all subdirectories in config directory
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(configDir, Files::isDirectory)) {
                for (Path collectionConfigDir : stream) {
                    String vectorName = collectionConfigDir.getFileName().toString();
                    Path configFile = collectionConfigDir.resolve("config.json");

                    if (Files.exists(configFile)) {
                        try {
                            // Read the JSON content as a Map first
                            Map<String, Object> configMap = mapper.readValue(configFile.toFile(),
                                    new TypeReference<Map<String, Object>>() {});

                            // Convert the distance string to Distance enum
                            String distanceStr = (String) configMap.get("distance");
                            Distance distance = Distance.valueOf(distanceStr);

                            // Create CollectionConfig using the existing fromMap method
                            Map<String, Object> requestBody = new HashMap<>();
                            requestBody.put("size", configMap.get("size"));
                            requestBody.put("distance", distanceStr);
                            requestBody.put("persist", String.valueOf(configMap.get("persist")));

                            CollectionConfig config = CollectionConfig.fromMap(vectorName, requestBody);

                            collectionConfigs.put(vectorName, config);
                            logger.info("Fetched config for vector store: {}", vectorName);

                        } catch (IOException e) {
                            logger.error("Failed to read config file for {}: {}. Config content: {}",
                                    vectorName, e.getMessage(),
                                    Files.readString(configFile));
                        } catch (IllegalArgumentException e) {
                            logger.error("Invalid distance metric in config for {}: {}",
                                    vectorName, e.getMessage());
                        }
                    } else {
                        logger.warn("Config file does not exist for vector store: {}", vectorName);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Failed to fetch configs: {}", e.getMessage());
            throw new RuntimeException("Failed to fetch configs", e);
        }
        return collectionConfigs;
    }

    public String addPayloadToVectorStore(String vectorName, Point point) {
        logger.info("Adding payload to VectorStore: {}", vectorName);
        try {
            // Ensure collection exists
            ensureCollectionExists(vectorName);

            Path vectorPath = Paths.get(COLLECTIONS_DIR, vectorName, "vectors.jsonl");

            // Append the point as a single line JSON
            String jsonLine = mapper.writeValueAsString(point) + "\n";
            Files.write(vectorPath, jsonLine.getBytes(), StandardOpenOption.APPEND);

            logger.info("Payload added to VectorStore: {}", vectorName);
            return "Payload added successfully to " + vectorName;
        } catch (IOException e) {
            logger.error("Failed to add payload: {}", e.getMessage());
            throw new RuntimeException("Failed to add payload to VectorStore", e);
        }
    }

    public List<Point> getAllPointsFromVectorStore(String vectorName) {
        List<Point> points = new ArrayList<>();
        Path vectorPath = Paths.get(COLLECTIONS_DIR, vectorName, "vectors.jsonl");

        try {
            if (!Files.exists(vectorPath)) {
                logger.warn("Vector store file does not exist: {}", vectorName);
                return points;
            }

            try (BufferedReader reader = Files.newBufferedReader(vectorPath)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) {
                        try {
                            // Read each line as a Map first
                            Map<String, Object> pointMap = mapper.readValue(line,
                                    new TypeReference<Map<String, Object>>() {});

                            // Extract values from the map
                            String id = (String) pointMap.get("id");
                            String content = (String) pointMap.get("content");
                            @SuppressWarnings("unchecked")
                            List<Number> vectorValues = (List<Number>) pointMap.get("vector");
                            @SuppressWarnings("unchecked")
                            Map<String, Object> metadata = (Map<String, Object>) pointMap.get("metadata");

                            // Convert List<Number> to float[]
                            float[] vector = null;
                            if (vectorValues != null) {
                                vector = new float[vectorValues.size()];
                                for (int i = 0; i < vectorValues.size(); i++) {
                                    vector[i] = vectorValues.get(i).floatValue();
                                }
                            }

                            // Create Point object using the appropriate constructor
                            Point point;
                            if (metadata != null) {
                                point = new Point(UUID.fromString(id), content, vector, metadata);
                            } else {
                                point = new Point(UUID.fromString(id), content, vector);
                            }

                            points.add(point);

                        } catch (Exception e) {
                            logger.error("Failed to parse point from line: {}. Error: {}", line, e.getMessage());
                            continue;
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Error while fetching points for vector store {}: {}", vectorName, e.getMessage());
            throw new RuntimeException("Failed to read points from vector store", e);
        }
        return points;
    }

    public List<Point> getAllPointsFromVectorStoreWithFilter(String vectorName, Map<String, Object> metadata) {
        return getAllPointsFromVectorStore(vectorName).stream()
                .filter(point -> matchesMetadata(point, metadata))
                .collect(Collectors.toList());
    }

    private boolean matchesMetadata(Point point, Map<String, Object> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return true;
        }

        Map<String, Object> pointMetadata = point.getMetadata();
        if (pointMetadata == null) {
            return false;
        }

        return metadata.entrySet().stream().allMatch(entry -> {
            String key = entry.getKey();
            Object filterValue = entry.getValue();
            Object pointValue = pointMetadata.get(key);

            if (pointValue == null) {
                return false;
            }

            if (filterValue instanceof String) {
                return filterValue.toString().equals(pointValue.toString());
            } else if (filterValue instanceof Number) {
                double filterNum = ((Number) filterValue).doubleValue();
                double pointNum = ((Number) pointValue).doubleValue();
                return Double.compare(filterNum, pointNum) == 0;
            } else if (filterValue instanceof Boolean) {
                return filterValue.equals(pointValue);
            } else if (filterValue instanceof Collection) {
                return ((Collection<?>) filterValue).contains(pointValue);
            }

            return filterValue.equals(pointValue);
        });
    }

    @Override
    public boolean removeVector(String collectionName) {
        logger.info("Removing vector and config for collection: {}", collectionName);
        AtomicBoolean success = new AtomicBoolean(true);

        try {
            // Remove vector directory and all its contents
            Path vectorDir = Paths.get(COLLECTIONS_DIR, collectionName);
            if (Files.exists(vectorDir)) {
                Files.walk(vectorDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                                logger.info("Deleted: {}", path);
                            } catch (IOException e) {
                                logger.error("Failed to delete: {}", path);
                                success.set(false);
                            }
                        });
            }

            // Remove config directory and all its contents
            Path configDir = Paths.get(CONFIG_DIR, collectionName);
            if (Files.exists(configDir)) {
                Files.walk(configDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                                logger.info("Deleted: {}", path);
                            } catch (IOException e) {
                                logger.error("Failed to delete: {}", path);
                                success.set(false);
                            }
                        });
            }

            logger.info("Successfully removed vector and config for collection: {}", collectionName);
        } catch (IOException e) {
            logger.error("Error while removing vector or config for collection: {}", e.getMessage());
            success.set(false);
        }

        return success.get();
    }
}