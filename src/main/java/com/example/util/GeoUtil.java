package com.example.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.ScalarFunction;

public class GeoUtil {
    // geo boundaries of the area of NYC
    public static double LON_EAST = -73.7;
    public static double LON_WEST = -74.05;
    public static double LAT_NORTH = 41.0;
    public static double LAT_SOUTH = 40.5;

    // area width and height
    public static double LON_WIDTH = 74.05 - 73.7;
    public static double LAT_HEIGHT = 41.0 - 40.5;

    // delta step to create artificial grid overlay of NYC
    public static double DELTA_LON = 0.0014;
    public static double DELTA_LAT = 0.00125;

    // ( |LON_WEST| - |LON_EAST| ) / DELTA_LAT
    public static int NUMBER_OF_GRID_X = 250;
    // ( LAT_NORTH - LAT_SOUTH ) / DELTA_LON
    public static int NUMBER_OF_GRID_Y = 400;

    public static float DEG_LEN = 110.25f;

    public static boolean isInNYC(float lon, float lat) {
        return !(lon > LON_EAST || lon < LON_WEST) && !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }


    public static double getEuclideanDistance(float lon1, float lat1, float lon2, float lat2) {
        double x = lat1 - lat2;
        double y = (lon1 - lon2) * Math.cos(lat2);
        return (DEG_LEN * Math.sqrt(x*x + y*y));
    }

    public static int getDirectionAngle(
            float startLon, float startLat, float destLon, float destLat) {

        double x = destLat - startLat;
        double y = (destLon - startLon) * Math.cos(startLat);
        int degrees = (int)Math.toDegrees(Math.atan2(x, y)) + 179;

        return degrees;
    }


    public static int mapToGridCell(float lon, float lat) {
        int xIndex = (int)Math.floor((Math.abs(LON_WEST) - Math.abs(lon)) / DELTA_LON);
        int yIndex = (int)Math.floor((LAT_NORTH - lat) / DELTA_LAT);

        return xIndex + (yIndex * NUMBER_OF_GRID_X);
    }

    public static float getGridCellCenterLon(int gridCellId) {
        int xIndex = gridCellId % NUMBER_OF_GRID_X;
        return  (float)(Math.abs(LON_WEST) - (xIndex * DELTA_LON) - (DELTA_LON / 2)) * -1.0F;
    }

    public static float getGridCellCenterLat(int gridCellId) {
        int xIndex = gridCellId % NUMBER_OF_GRID_X;
        int yIndex = (gridCellId - xIndex) / NUMBER_OF_GRID_X;

        return (float)(LAT_NORTH - (yIndex * DELTA_LAT) - (DELTA_LAT / 2));
    }

    public static class IsInNYC extends ScalarFunction {
        public boolean eval(float lon, float lat) { return isInNYC(lon, lat); }
    }

    public static class ToCellId extends ScalarFunction {
        public int eval(float lon, float lat) { return mapToGridCell(lon, lat); }
    }

    public static class ToCoords extends ScalarFunction {
        public Tuple2<Float, Float> eval(int cellId) {
            return Tuple2.of(
                    getGridCellCenterLon(cellId),
                    getGridCellCenterLat(cellId)
            );
        }
    }




}
