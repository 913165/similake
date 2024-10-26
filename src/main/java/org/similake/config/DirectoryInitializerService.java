package org.similake.config;

import jakarta.annotation.PostConstruct;
import org.similake.persist.RocksDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class DirectoryInitializerService {

    private static final Logger logger = LoggerFactory.getLogger(DirectoryInitializerService.class);

    @Value("${db.path}")
    private String COLLECTIONS_DIR;

    @Value("${config.path}")
    private String CONFIG_DIR;

    @PostConstruct
    public void initDirectories() {
        cleanAllDirectories();
        createDirectoryIfNotExists(COLLECTIONS_DIR);
        createDirectoryIfNotExists(CONFIG_DIR);
    }

    private void createDirectoryIfNotExists(String dirPath) {
        Path path = Paths.get(dirPath);
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
                logger.info("Directory created: {}", dirPath);
            } catch (IOException e) {
                logger.error("Error creating directory: " + dirPath, e);
            }
        } else {
            logger.info("Directory already exists: {}", dirPath);
        }
    }

    public void cleanAllDirectories() {
        cleanDirectory(COLLECTIONS_DIR);
        cleanDirectory(CONFIG_DIR);
    }

    public void cleanDirectory(String dirPath) {
        Path path = Paths.get(dirPath);
        if (Files.exists(path)) {
            try {
                AtomicInteger deletedCount = new AtomicInteger(0);
                Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        deletedCount.incrementAndGet();
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        if (!dir.equals(path)) {  // Don't delete the root directory
                            Files.delete(dir);
                            deletedCount.incrementAndGet();
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });
                logger.info("Cleaned directory: {}, {} items deleted", dirPath, deletedCount.get());
            } catch (IOException e) {
                logger.error("Error cleaning directory: " + dirPath, e);
                throw new RuntimeException("Failed to clean directory: " + dirPath, e);
            }
        } else {
            logger.warn("Directory does not exist: {}", dirPath);
        }
    }
}