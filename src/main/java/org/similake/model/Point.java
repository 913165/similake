package org.similake.model;

import java.util.Arrays;
import java.util.UUID;

public class Point {
    // Fields representing the JSON structure
    private UUID id;
    private String content;
    private float[] vector;

    // Constructor to initialize fields
    public Point(UUID id, String content, float[] vector) {
        this.id = id;
        this.content = content;
        this.vector = vector;
    }

    // Getters and setters
    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public float[] getVector() {
        return vector;
    }

    public void setVector(float[] vector) {
        this.vector = vector;
    }

    // Method to display the Point data
    @Override
    public String toString() {
        return "Point{" +
                "id=" + id +
                ", content='" + content + '\'' +
                ", vector=" + Arrays.toString(vector) +
                '}';
    }
}
