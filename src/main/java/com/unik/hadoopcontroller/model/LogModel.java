package com.unik.hadoopcontroller.model;

import java.io.FileWriter;
import java.io.IOException;

public class LogModel {
    private FileWriter logWriter;

    public LogModel(String logFilePath) {
        try {
            logWriter = new FileWriter(logFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void logInfo(String message) {
        writeLog("[INFO] " + message);
    }

    public void logWarning(String message) {
        writeLog("[WARNING] " + message);
    }

    public void logError(String message) {
        writeLog("[ERROR] " + message);
    }

    private void writeLog(String logMessage) {
        try {
            logWriter.write(logMessage + "\n");
            logWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            logWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

