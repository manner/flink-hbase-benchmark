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

    public abstract Source<?, ? ,?> makeSource(StreamExecutionEnvironment env, BenchmarkTarget target);
    public abstract DataStream<?> makeMapper(DataStream<?> in, BenchmarkTarget target);
    public abstract Sink<?, ?, ?, ?> makeSink(DataStream<?> in, BenchmarkTarget target);

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
        public Source<?, ?, ?> makeSource(StreamExecutionEnvironment env, BenchmarkTarget target) {
            return target.makeSourceForThroughput(env);
        }

        @Override
        public DataStream<?> makeMapper(DataStream<?> in, BenchmarkTarget target) {
            return target.makeMapperForThroughput(in);
        }

        @Override
        public Sink<?, ?, ?, ?> makeSink(DataStream<?> in, BenchmarkTarget target) {
            return target.makeSinkForThroughput(in);
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
        public Source<?, ?, ?> makeSource(StreamExecutionEnvironment env, BenchmarkTarget target) {
            return target.makeSourceForLatency(env);
        }

        @Override
        public DataStream<?> makeMapper(DataStream<?> in, BenchmarkTarget target) {
            return target.makeMapperForLatency(in);
        }

        @Override
        public Sink<?, ?, ?, ?> makeSink(DataStream<?> in, BenchmarkTarget target) {
            return target.makeSinkForLatency(in);
        }
    }

}
