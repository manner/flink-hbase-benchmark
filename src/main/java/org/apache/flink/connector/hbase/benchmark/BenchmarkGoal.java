package org.apache.flink.connector.hbase.benchmark;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

public abstract class BenchmarkGoal {

    public abstract void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target);

    public abstract void makeData(String tableName, int numberOfColumns, BenchmarkTarget target);

    public abstract <T> DataStream<T> makeStreamFromSource(StreamExecutionEnvironment env, BenchmarkTarget<T> target, String id);

    public abstract <T> DataStream<T> makeMapper(DataStream<T> in, BenchmarkTarget<T> target);

    public abstract <T> void sinkStream(DataStream<T> in, BenchmarkTarget<T> target);

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
        public <T> DataStream<T> makeMapper(DataStream<T> in, BenchmarkTarget<T> target) {
            return target.makeMapperForThroughput(in);
        }

        @Override
        public <T> void sinkStream(DataStream<T> in, BenchmarkTarget<T> target) {
            target.sinkForThroughput(in);
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
        public <T> DataStream<T> makeMapper(DataStream<T> in, BenchmarkTarget<T> target) {
            return target.makeMapperForLatency(in);
        }

        @Override
        public <T> void sinkStream(DataStream<T> in, BenchmarkTarget<T> target) {
            target.sinkForLatency(in);
        }
    }

}
