package org.apache.flink.connector.hbase.benchmark;


import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

public class CSVWriter implements Serializable {

    private final String resultPath;

    public CSVWriter(File resultFolder, String[] header) {
        this.resultPath = resultFolder.toPath().resolve(UUID.randomUUID().toString()+".csv").toAbsolutePath().toString();
        try {
            resultPath().toFile().createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        writeRow(header);
    }


    public void writeRow(String... cells) {
        String lineToWrite = String.join(",", cells) + "\n";
        try {
            Files.write(resultPath(), lineToWrite.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Path resultPath() {
        return Paths.get(resultPath);
    }
}
