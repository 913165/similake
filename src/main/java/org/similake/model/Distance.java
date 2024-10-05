package org.similake.model;

/**
 * Enum to represent different types of distance measures with corresponding numeric values.
 * 
 * @see <a href="https://en.wikipedia.org/wiki/Cosine_similarity">Cosine Similarity</a>
 * @see <a href="https://en.wikipedia.org/wiki/Euclidean_distance">Euclidean Distance</a>
 * @see <a href="https://en.wikipedia.org/wiki/Dot_product">Dot Product</a>
 * @see <a href="https://simple.wikipedia.org/wiki/Manhattan_distance">Manhattan Distance</a>
 */
public enum Distance {
    Cosine(1),   // Cosine similarity
    Euclid(2),   // Euclidean distance
    Dot(3),      // Dot product
    Manhattan(4); // Manhattan distance

    private final int value;

    // Constructor to assign numeric value to each distance type
    Distance(int value) {
        this.value = value;
    }

    // Getter to retrieve the numeric value
    public int getValue() {
        return value;
    }
}
