package org.similake.model;

import java.util.ArrayList;
import java.util.List;

public class VectorStore {
    // Fields representing size and distance calculation type
    private int size;
    private Distance distanceType;
    private List<Point> points;


    // Constructor to initialize VectorStore with size and distance type
    public VectorStore(int size, Distance distanceType) {
        this.size = size;
        this.distanceType = distanceType;
        this.points = new ArrayList<>();
    }

    // Method to add a Point to the store
    public void addPoint(Point point) {
        this.points.add(point);
    }

    // Getter for size
    public int getSize() {
        return size;
    }

    // Getter for distance type
    public Distance getDistanceType() {
        return distanceType;
    }

    // Getter for the list of points
    public List<Point> getPoints() {
        return points;
    }


}