package com.example.util;

import org.apache.commons.math3.stat.regression.SimpleRegression;

public class TravelTimePredictionModel {
    private static int NUM_DIRECTION_BUCKETS = 8;
    private static int BUCKET_ANGLE = 360 / NUM_DIRECTION_BUCKETS;

    SimpleRegression[] models;

    public TravelTimePredictionModel() {
        models = new SimpleRegression[NUM_DIRECTION_BUCKETS];
        for (int i = 0; i < NUM_DIRECTION_BUCKETS; i++) {
            models[i] = new SimpleRegression(false);
        }
    }

    public int predictTravelTime(int direction, double distance) {
        byte directionBucket = getDirectionBucket(direction);
        double prediction = models[directionBucket].predict(distance);

        if (Double.isNaN(prediction)) {
            return -1;
        } else {
            return (int)prediction;
        }
    }

    public void refineModel(int direction, double distance, double travelTime) {
        byte directionBucket = getDirectionBucket(direction);
        models[directionBucket].addData(distance, travelTime);
    }

    private byte getDirectionBucket(int direction) { return (byte)(direction/BUCKET_ANGLE); }
}
