package org.apache.flink.connector.hbase.benchmark;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;

public abstract class BenchmarkGoal {

    public abstract void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target);
    public abstract void makeData(String tableName, int numberOfColumns, BenchmarkTarget target);

    public abstract <T> Source<T, ? ,?> makeSource(StreamExecutionEnvironment env, BenchmarkTarget<T> target);
    public abstract <T> DataStream<T> makeMapper(DataStream<T> in, BenchmarkTarget<T> target);
    public abstract <T> Sink<T, ?, ?, ?> makeSink(BenchmarkTarget<T> target);

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
        public <T> Source<T, ?, ?> makeSource(StreamExecutionEnvironment env, BenchmarkTarget<T> target) {
            return target.makeSourceForThroughput(env);
        }

        @Override
        public <T> DataStream<T> makeMapper(DataStream<T> in, BenchmarkTarget<T> target) {
            return target.makeMapperForThroughput(in);
        }

        @Override
        public <T> Sink<T, ?, ?, ?> makeSink(BenchmarkTarget<T> target) {
            return target.makeSinkForThroughput();
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
        public <T> Source<T, ?, ?> makeSource(StreamExecutionEnvironment env, BenchmarkTarget<T> target) {
            return target.makeSourceForLatency(env);
        }

        @Override
        public <T> DataStream<T> makeMapper(DataStream<T> in, BenchmarkTarget<T> target) {
            return target.makeMapperForLatency(in);
        }

        @Override
        public <T> Sink<T, ?, ?, ?> makeSink(BenchmarkTarget<T> target) {
            return target.makeSinkForLatency();
        }
    }

}
