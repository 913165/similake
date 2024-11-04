package org.similake.sample;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class JsonFileReader {
    private static final String API_ENDPOINT = "http://localhost:6767/collections/vector_store/payload";

    public static List<JSONObject> readJsonArrayFile(String filePath) {
        List<JSONObject> jsonElements = new ArrayList<>();
        JSONParser parser = new JSONParser();

        try (FileReader reader = new FileReader(new File(filePath))) {
            JSONArray jsonArray = (JSONArray) parser.parse(reader);

            for (Object obj : jsonArray) {
                if (obj instanceof JSONObject) {
                    jsonElements.add((JSONObject) obj);
                }
            }

        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            e.printStackTrace();
        } catch (ParseException e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
            e.printStackTrace();
        }

        return jsonElements;
    }

    public static void sendToEndpoint(String jsonPayload) {
        try {
            URL url = new URL(API_ENDPOINT);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // Set up the HTTP POST request
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);

            // Send the JSON payload
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonPayload.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            // Get the response
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK || responseCode == HttpURLConnection.HTTP_CREATED) {
                System.out.println("Successfully sent payload. Response code: " + responseCode);
            } else {
                System.err.println("Failed to send payload. Response code: " + responseCode);
            }

            connection.disconnect();

        } catch (Exception e) {
            System.err.println("Error sending payload to endpoint: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String filePath = "C:\\Users\\tinum\\IdeaProjects\\similake-client-springboot\\src\\main\\resources\\product_50.json";

        // Start timing
        Instant startTime = Instant.now();

        // Read the file
        System.out.println("Starting to read file at: " + startTime);
        List<JSONObject> elements = readJsonArrayFile(filePath);

        // Record file reading time
        Instant afterReading = Instant.now();
        Duration readingTime = Duration.between(startTime, afterReading);
        System.out.println("\nFile reading statistics:");
        System.out.println("Total elements read: " + elements.size());
        System.out.println("Time taken to read file: " + readingTime.toMillis() + "ms");

        // Process and send each JSON object to the endpoint
        int successCount = 0;
        int failureCount = 0;

        System.out.println("\nStarting to send elements at: " + afterReading);

        for (int i = 0; i < elements.size(); i++) {
            try {
                JSONObject element = elements.get(i);
                String jsonPayload = element.toJSONString();

                // Record start time for this element
                Instant elementStartTime = Instant.now();

                // Send to endpoint
                sendToEndpoint(jsonPayload);

                // Record end time and calculate duration for this element
                Instant elementEndTime = Instant.now();
                Duration elementDuration = Duration.between(elementStartTime, elementEndTime);

                System.out.printf("Element %d/%d processed in %dms%n",
                        i + 1, elements.size(), elementDuration.toMillis());

                successCount++;

                // Optional: Add delay between requests
                Thread.sleep(100);

            } catch (InterruptedException e) {
                System.err.println("Thread interrupted: " + e.getMessage());
                Thread.currentThread().interrupt();
                failureCount++;
            } catch (Exception e) {
                System.err.println("Error processing element: " + e.getMessage());
                failureCount++;
            }
        }

        // Calculate total time
        Instant endTime = Instant.now();
        Duration totalTime = Duration.between(startTime, endTime);
        Duration processingTime = Duration.between(afterReading, endTime);

        System.out.println("File reading time: " + readingTime.toSeconds() + "s");
        System.out.println("Processing and sending time: " + processingTime.toSeconds() + "s");
        System.out.println("Total execution time: " + totalTime.toSeconds() + "s");
        System.out.println("Average time per element: " +
                (elements.size() > 0 ? processingTime.toSeconds() / elements.size() : 0) + "s");
    }
}