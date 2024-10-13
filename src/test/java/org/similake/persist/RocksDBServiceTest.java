package org.similake.persist;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.similake.collections.config.CollectionConfig;
import org.similake.model.Distance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ActiveProfiles("test")
@SpringBootTest
class RocksDBServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBServiceTest.class);

    @Value("${db.path}")
    private String COLLECTIONS_DIR;

    @Value("${config.path}")
    private String CONFIG_DIR;

    @Autowired
    private RocksDBService rocksDBService;

    @BeforeEach
    void setUp() throws IOException {
        createDirectoryIfNotExists(COLLECTIONS_DIR);
        createDirectoryIfNotExists(CONFIG_DIR);
    }

    private void createDirectoryIfNotExists(String dirPath) throws IOException {
        logger.info("Creating directory if not exists: {}", dirPath);
        Path path = Paths.get(dirPath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        deleteDirectoryIfExists(COLLECTIONS_DIR);
        deleteDirectoryIfExists(CONFIG_DIR);
    }

    private void deleteDirectoryIfExists(String dirPath) throws IOException {
        logger.info("Deleting directory if exists: {}", dirPath);
        Path path = Paths.get(dirPath);
        if (Files.exists(path)) {
            try {
                Files.walkFileTree(path, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (NoSuchFileException | DirectoryNotEmptyException e) {
                logger.error("Error deleting directory: {}", dirPath, e);
            }
        }
    }

    @Test
    void persistVectorToStorage() {
        String collectionName = "test_collection";
        CollectionConfig config = new CollectionConfig(collectionName, 100, Distance.Cosine, true);

        // Call the method to persist the vector to storage
        String result = rocksDBService.persistVectorToStorage(collectionName, config);

        // Verify that the method returned the expected success message
        assertEquals("Created and persisted collection successfully: " + collectionName, result, "Persisting vector to storage should return success message");

        // Fetch the config back from storage
        CollectionConfig fetchedConfig = rocksDBService.fetchVectorFromStorage(collectionName);

        // Verify that the fetched config is not null and matches the original config
        assertNotNull(fetchedConfig, "Fetched config should not be null");
        assertEquals(config.getCollectionName(), fetchedConfig.getCollectionName(), "Collection names should match");
        assertEquals(config.getSize(), fetchedConfig.getSize(), "Sizes should match");
        assertEquals(config.getDistance(), fetchedConfig.getDistance(), "Distances should match");
        assertEquals(config.isPersist(), fetchedConfig.isPersist(), "Persist flags should match");
    }

    @Test
    void createConfig() {
        String collectionName = "vector_store";
        CollectionConfig config = new CollectionConfig(collectionName, 100, Distance.Cosine, true);

        // Call the method to persist the config
        boolean result = rocksDBService.createConfig(collectionName, config);

        // Verify that the method returned true
        assertTrue(result, "createConfig should return true");

        // Fetch the config back from storage
        CollectionConfig fetchedConfig = rocksDBService.fetchVectorFromStorage(collectionName);

        // Verify that the fetched config is not null and matches the original config
        assertNotNull(fetchedConfig, "Fetched config should not be null");
        assertEquals(config.getCollectionName(), fetchedConfig.getCollectionName(), "Collection names should match");
        assertEquals(config.getSize(), fetchedConfig.getSize(), "Sizes should match");
        assertEquals(config.getDistance(), fetchedConfig.getDistance(), "Distances should match");
        assertEquals(config.isPersist(), fetchedConfig.isPersist(), "Persist flags should match");
    }

    @Test
    void fetchVectorFromStorage() {
        String collectionName = "test_collection";
        CollectionConfig config = new CollectionConfig(collectionName, 100, Distance.Cosine, true);

        // Persist the config to storage
        rocksDBService.createConfig(collectionName, config);

        // Fetch the config back from storage
        CollectionConfig fetchedConfig = rocksDBService.fetchVectorFromStorage(collectionName);

        // Verify that the fetched config is not null and matches the original config
        assertNotNull(fetchedConfig, "Fetched config should not be null");
        assertEquals(config.getCollectionName(), fetchedConfig.getCollectionName(), "Collection names should match");
        assertEquals(config.getSize(), fetchedConfig.getSize(), "Sizes should match");
        assertEquals(config.getDistance(), fetchedConfig.getDistance(), "Distances should match");
        assertEquals(config.isPersist(), fetchedConfig.isPersist(), "Persist flags should match");
    }

    @Test
    void fetchAllCollectionConfigs() {
        // Create some test collection configs
        String collectionName1 = "test_collection_1";
        String collectionName2 = "test_collection_2";
        CollectionConfig config1 = new CollectionConfig(collectionName1, 100, Distance.Cosine, true);
        CollectionConfig config2 = new CollectionConfig(collectionName2, 200, Distance.Euclidean, false);

        // Persist the test collection configs
        rocksDBService.createConfig(collectionName1, config1);
        rocksDBService.createConfig(collectionName2, config2);

        // Fetch all collection configs
        Map<String, CollectionConfig> allConfigs = rocksDBService.fetchAllCollectionConfigs();

        // Verify that the fetched configs are not null and match the original configs
        assertNotNull(allConfigs, "Fetched configs should not be null");
        assertEquals(2, allConfigs.size(), "There should be 2 collection configs");

        CollectionConfig fetchedConfig1 = allConfigs.get(collectionName1);
        CollectionConfig fetchedConfig2 = allConfigs.get(collectionName2);

        assertNotNull(fetchedConfig1, "Fetched config1 should not be null");
        assertEquals(config1.getCollectionName(), fetchedConfig1.getCollectionName(), "Collection names should match for config1");
        assertEquals(config1.getSize(), fetchedConfig1.getSize(), "Sizes should match for config1");
        assertEquals(config1.getDistance(), fetchedConfig1.getDistance(), "Distances should match for config1");
        assertEquals(config1.isPersist(), fetchedConfig1.isPersist(), "Persist flags should match for config1");

        assertNotNull(fetchedConfig2, "Fetched config2 should not be null");
        assertEquals(config2.getCollectionName(), fetchedConfig2.getCollectionName(), "Collection names should match for config2");
        assertEquals(config2.getSize(), fetchedConfig2.getSize(), "Sizes should match for config2");
        assertEquals(config2.getDistance(), fetchedConfig2.getDistance(), "Distances should match for config2");
        assertEquals(config2.isPersist(), fetchedConfig2.isPersist(), "Persist flags should match for config2");
    }
}