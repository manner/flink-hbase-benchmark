package org.apache.flink.connector.hbase.benchmark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

public abstract class BenchmarkGoal {

    public abstract void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target);

    public abstract void makeData(String tableName, int numberOfColumns, BenchmarkTarget target);

    public abstract <T> DataStream<T> makeStreamFromSource(StreamExecutionEnvironment env, BenchmarkTarget<T> target, String id);

    public abstract <T> DataStream<T> makeMapper(DataStream<T> in, BenchmarkTarget<T> target, File resultFolder);

    public abstract <T> void sinkStream(DataStream<T> in, BenchmarkTarget<T> target, String tableName);

    public abstract void retrieveResults(BenchmarkTarget target, String tableName, File resultFolder);

    public static class Throughput extends BenchmarkGoal {
        @Override
        public void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target) {
            // Ignore, basic tables are enough for throughput testing
        }

        @Override
        public void makeData(String tableName, int numberOfColumns, BenchmarkTarget target) {
            target.makeDataForThroughput(tableName, numberOfColumns);
        }

        @Override
        public <T> DataStream<T> makeStreamFromSource(StreamExecutionEnvironment env, BenchmarkTarget<T> target, String id) {
            return target.makeStreamFromSourceForThroughput(env, id);
        }

        @Override
        public <T> DataStream<T> makeMapper(DataStream<T> in, BenchmarkTarget<T> target, File resultFolder) {
            return in.map(new ThroughputMapper<>(resultFolder));
        }

        @Override
        public <T> void sinkStream(DataStream<T> in, BenchmarkTarget<T> target, String tableName) {
            target.sinkForThroughput(in, tableName);
        }

        @Override
        public void retrieveResults(BenchmarkTarget target, String tableName, File resultFolder) {
            // Results are written by mapper
        }
    }

    public static class Latency extends BenchmarkGoal {

        @Override
        public void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target) {
            target.augmentTableDescriptorForLatency(basicTableDescriptor);
        }

        @Override
        public void makeData(String tableName, int numberOfColumns, BenchmarkTarget target) {
            target.makeDataForLatency(tableName, numberOfColumns);
        }

        @Override
        public <T> DataStream<T> makeStreamFromSource(StreamExecutionEnvironment env, BenchmarkTarget<T> target, String id) {
            return target.makeStreamFromSourceForLatency(env, id);
        }

        @Override
        public <T> DataStream<T> makeMapper(DataStream<T> in, BenchmarkTarget<T> target, File resultFolder) {
            return target.makeMapperForLatency(in, resultFolder);
        }

        @Override
        public <T> void sinkStream(DataStream<T> in, BenchmarkTarget<T> target, String tableName) {
            target.sinkForLatency(in, tableName);
        }

        @Override
        public void retrieveResults(BenchmarkTarget target, String tableName, File resultFolder) {
            target.retrieveResultsForLatency(tableName, resultFolder);
        }
    }


    public static class ThroughputMapper<T> implements MapFunction<T, T> {

        public static final int RESOLUTION = 1_000_000; //TODO think bigger

        private int count = 0;
        private long lastTimeStamp = -1;
        private final String resultPath;

        public ThroughputMapper(File resultFolder) {
            this.resultPath = resultFolder.toPath().resolve(UUID.randomUUID().toString()+".csv").toAbsolutePath().toString();
            try {
                resultPath().toFile().createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            String[] header = new String[]{"resolution", "time"};
            writeRow(header);
        }

        @Override
        public T map(T t) throws Exception {
            count++;
            if (count % RESOLUTION == 0) {
                long current = System.currentTimeMillis();
                if (lastTimeStamp < 0) {
                    //First time
                } else {
                    long diff = current - lastTimeStamp;
                    writeRow(""+RESOLUTION, ""+diff);
                }
                lastTimeStamp = current;
            }
            return t;
        }

        private void writeRow(String... cells) {
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

}
