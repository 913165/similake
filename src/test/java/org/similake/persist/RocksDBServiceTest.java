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


}