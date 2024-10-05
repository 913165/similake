package org.similake.model;

public class DenseVector {
    private int dim;
    private Distance distance;

    public DenseVector(int dim, Distance distance) {
        this.dim = dim;
        this.distance = distance;
    }

    public int getDim() {
        return dim;
    }

    public void setDim(int dim) {
        this.dim = dim;
    }

    public Distance getDistance() {
        return distance;
    }

    public void setDistance(Distance distance) {
        this.distance = distance;
    }

    @Override
    public String toString() {
        return "DenseVector{" +
                "dim=" + dim +
                ", distance=" + distance +
                '}';
    }
}