package org.apache.flink.connector.hbase.benchmark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

public abstract class BenchmarkTarget {

    /** Create table name for this target, overridable for explicit naming patterns*/
    public String createTableName() {
        return "table-"+ UUID.randomUUID();
    }

    public abstract void augmentTableDescriptorForLatency(TableDescriptorBuilder basicTableDescriptor);
    public abstract void makeDataForLatency(String tableName, int numberOfColumns);
    public abstract void makeDataForThroughput(String tableName, int numberOfColumns);

    public abstract org.apache.flink.api.connector.source.Source<?,?,?> makeSourceForThroughput(StreamExecutionEnvironment env);
    public abstract DataStream<?> makeMapperForThroughput(DataStream<?> in);
    public abstract org.apache.flink.api.connector.sink.Sink<?,?,?,?> makeSinkForThroughput(DataStream<?> in);

    public abstract org.apache.flink.api.connector.source.Source<?,?,?> makeSourceForLatency(StreamExecutionEnvironment env);
    public abstract DataStream<?> makeMapperForLatency(DataStream<?> in);
    public abstract org.apache.flink.api.connector.sink.Sink<?,?,?,?> makeSinkForLatency(DataStream<?> in);

    public static class Source extends BenchmarkTarget {
        @Override
        public void augmentTableDescriptorForLatency(TableDescriptorBuilder basicTableDescriptor) {
            // Can be ignored, latency testing just needs the n column families
        }

        @Override
        public void makeDataForLatency(String tableName, int numberOfColumns) {
            //TODO
        }

        @Override
        public void makeDataForThroughput(String tableName, int numberOfColumns) {
            Main.runHBasePerformanceEvaluator(tableName, numberOfColumns, 100000, 1);
        }

        @Override
        public org.apache.flink.api.connector.source.Source<?, ?, ?> makeSourceForThroughput(StreamExecutionEnvironment env) {
            //TODO
            return null;
        }

        @Override
        public DataStream<?> makeMapperForThroughput(DataStream<?> in) {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<?, ?, ?, ?> makeSinkForThroughput(DataStream<?> in) {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.source.Source<?, ?, ?> makeSourceForLatency(StreamExecutionEnvironment env) {
            //TODO
            return null;
        }

        @Override
        public DataStream<?> makeMapperForLatency(DataStream<?> in) {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<?, ?, ?, ?> makeSinkForLatency(DataStream<?> in) {
            //TODO
            return null;
        }


    }
    public static class Sink extends BenchmarkTarget {
        public static final String CREATION_TIMESTAMP_CF = "creation_timestamp";
        @Override
        public void augmentTableDescriptorForLatency(TableDescriptorBuilder basicTableDescriptor) {
            basicTableDescriptor.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CREATION_TIMESTAMP_CF));
        }

        @Override
        public void makeDataForLatency(String tableName, int numberOfColumns) {
            // Shouldn't do anything. No need for creating data in HBase when testing the Sink only.
        }

        @Override
        public void makeDataForThroughput(String tableName, int numberOfColumns) {
            // Shouldn't do anything. No need for creating data in HBase when testing the Sink only.
        }

        @Override
        public org.apache.flink.api.connector.source.Source<?, ?, ?> makeSourceForThroughput(StreamExecutionEnvironment env) {
            return new NumberSequenceSource(0,10000);
        }

        @Override
        public DataStream<?> makeMapperForThroughput(DataStream<?> in) {
            //WIP
            return in.map((MapFunction<Long, Long>) value -> System.currentTimeMillis());
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<?, ?, ?, ?> makeSinkForThroughput(DataStream<?> in) {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.source.Source<?, ?, ?> makeSourceForLatency(StreamExecutionEnvironment env) {
            return new NumberSequenceSource(0,10000);
        }

        @Override
        public DataStream<?> makeMapperForLatency(DataStream<?> in) {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<?, ?, ?, ?> makeSinkForLatency(DataStream<?> in) {
            //TODO
            return null;
        }
    }

}
