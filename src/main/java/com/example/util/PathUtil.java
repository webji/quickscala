package com.example.util;

import org.apache.commons.io.FileUtils;
import org.apache.flink.core.fs.Path;

import java.io.File;

public class PathUtil {
    public static String currentPath() {
        File dir = new File(".");
        return dir.getAbsolutePath();
    }
}
