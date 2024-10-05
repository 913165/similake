package org.similake.model;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Payload {

    private String id;
    private Map<String, Object> metadata;
    private String content;
    private List<String> media;
    private float[] embedding = new float[0];

    // Default constructor
    public Payload() {
        // Generating a random UUID for the document if not provided
        this.id = UUID.randomUUID().toString();
    }

    // Parameterized constructor
  public Payload(String id, Map<String, Object> metadata, String content, List<String> media, float[] embedding) {
    this.id = id;
    this.metadata = metadata;
    this.content = content;
    this.media = media;
    this.embedding = embedding;
}



    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public List<String> getMedia() {
        return media;
    }

    public void setMedia(List<String> media) {
        this.media = media;
    }

    public float[] getEmbedding() {
        return embedding;
    }


    // Overriding toString method to print Document details
    @Override
    public String toString() {
        return "Document{" +
                "id='" + id + '\'' +
                ", metadata=" + metadata +
                ", content='" + content + '\'' +
                ", media=" + media +
                '}';
    }
}
