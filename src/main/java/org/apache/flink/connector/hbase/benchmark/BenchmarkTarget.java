package org.apache.flink.connector.hbase.benchmark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

public abstract class BenchmarkTarget<StreamType> {

    /** Create table name for this target, overridable for explicit naming patterns*/
    public String createTableName() {
        return "table-"+ UUID.randomUUID();
    }

    public abstract void augmentTableDescriptorForLatency(TableDescriptorBuilder basicTableDescriptor);
    public abstract void makeDataForLatency(String tableName, int numberOfColumns);
    public abstract void makeDataForThroughput(String tableName, int numberOfColumns);

    public abstract org.apache.flink.api.connector.source.Source<StreamType,?,?> makeSourceForThroughput(StreamExecutionEnvironment env);
    public abstract DataStream<?> makeMapperForThroughput(DataStream<StreamType> in);
    public abstract org.apache.flink.api.connector.sink.Sink<StreamType,?,?,?> makeSinkForThroughput(DataStream<?> in);

    public abstract org.apache.flink.api.connector.source.Source<StreamType,?,?> makeSourceForLatency(StreamExecutionEnvironment env);
    public abstract DataStream<?> makeMapperForLatency(DataStream<StreamType> in);
    public abstract org.apache.flink.api.connector.sink.Sink<StreamType,?,?,?> makeSinkForLatency(DataStream<?> in);

    public static class Source extends BenchmarkTarget<HBaseEvent> {
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
        public org.apache.flink.api.connector.source.Source<HBaseEvent, ?, ?> makeSourceForThroughput(StreamExecutionEnvironment env) {
            //TODO
            return null;
        }

        @Override
        public DataStream<?> makeMapperForThroughput(DataStream<HBaseEvent> in) {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<HBaseEvent, ?, ?, ?> makeSinkForThroughput(DataStream<?> in) {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.source.Source<HBaseEvent, ?, ?> makeSourceForLatency(StreamExecutionEnvironment env) {
            //TODO
            return null;
        }

        @Override
        public DataStream<?> makeMapperForLatency(DataStream<HBaseEvent> in) {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<HBaseEvent, ?, ?, ?> makeSinkForLatency(DataStream<?> in) {
            //TODO
            return null;
        }


    }
    public static class Sink extends BenchmarkTarget<Long> {
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
        public org.apache.flink.api.connector.source.Source<Long, ?, ?> makeSourceForThroughput(StreamExecutionEnvironment env) {
            return new NumberSequenceSource(0,10000);
        }

        @Override
        public DataStream<?> makeMapperForThroughput(DataStream<Long> in) {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<Long, ?, ?, ?> makeSinkForThroughput(DataStream<?> in) {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.source.Source<Long, ?, ?> makeSourceForLatency(StreamExecutionEnvironment env) {
            return new NumberSequenceSource(0,10000);
        }

        @Override
        public DataStream<?> makeMapperForLatency(DataStream<Long> in) {
            return in.map((MapFunction<Long, Long>) value -> System.currentTimeMillis());
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<Long, ?, ?, ?> makeSinkForLatency(DataStream<?> in) {
            //TODO
            return null;
        }
    }

}
