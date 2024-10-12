package org.similake.config;

import jakarta.annotation.PostConstruct;
import org.similake.persist.RocksDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
public class DirectoryInitializerService {

    private static final Logger logger = LoggerFactory.getLogger(DirectoryInitializerService.class);


    @Value("${db.path}")
    private  String COLLECTIONS_DIR;

    @Value("${config.path}")
    private  String CONFIG_DIR;

   @PostConstruct
    public void initDirectories() {
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
}
