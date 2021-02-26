package org.apache.flink.connector.hbase.benchmark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.hbase.sink.HBaseSink;
import org.apache.flink.connector.hbase.sink.HBaseSinkSerializer;
import org.apache.flink.connector.hbase.source.HBaseSource;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.UUID;

public abstract class BenchmarkTarget<StreamType> {

    /**
     * Create table name for this target, overridable for explicit naming patterns
     */
    public String createTableName() {
        return "table-" + UUID.randomUUID();
    }

    public abstract void augmentTableDescriptorForLatency(TableDescriptorBuilder basicTableDescriptor);

    public abstract void makeDataForLatency(String tableName, int numberOfColumns);

    public abstract void makeDataForThroughput(String tableName, int numberOfColumns);

    public abstract org.apache.flink.api.connector.source.Source<StreamType, ?, ?> makeSourceForThroughput(StreamExecutionEnvironment env);

    public DataStream<StreamType> makeMapperForThroughput(DataStream<StreamType> in) {
        //TODO, common for both targets
        return null;
    }

    public abstract org.apache.flink.api.connector.sink.Sink<StreamType, ?, ?, ?> makeSinkForThroughput();


    public abstract org.apache.flink.api.connector.source.Source<StreamType, ?, ?> makeSourceForLatency(StreamExecutionEnvironment env);

    public abstract DataStream<StreamType> makeMapperForLatency(DataStream<StreamType> in);

    public abstract org.apache.flink.api.connector.sink.Sink<StreamType, ?, ?, ?> makeSinkForLatency();


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
            return new HBaseSource<>(
                    Boundedness.CONTINUOUS_UNBOUNDED,
                    new HBaseEventDeserializer(),
                    "tableName",
                    Main.HBASE_CONFIG);
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<HBaseEvent, ?, ?, ?> makeSinkForThroughput() {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.source.Source<HBaseEvent, ?, ?> makeSourceForLatency(StreamExecutionEnvironment env) {
            //TODO
            return null;
        }

        @Override
        public DataStream<HBaseEvent> makeMapperForLatency(DataStream<HBaseEvent> in) {
            //TODO
            return null;
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<HBaseEvent, ?, ?, ?> makeSinkForLatency() {
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
            return new NumberSequenceSource(0, 10000);
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<Long, ?, ?, ?> makeSinkForThroughput() {
            return new HBaseSink<>("tableName", new LongSerializer(), Main.HBASE_CONFIG);

        }

        @Override
        public org.apache.flink.api.connector.source.Source<Long, ?, ?> makeSourceForLatency(StreamExecutionEnvironment env) {
            return new NumberSequenceSource(0, 10000);
        }

        @Override
        public DataStream<Long> makeMapperForLatency(DataStream<Long> in) {
            return in.map((MapFunction<Long, Long>) value -> System.currentTimeMillis());
        }

        @Override
        public org.apache.flink.api.connector.sink.Sink<Long, ?, ?, ?> makeSinkForLatency() {
            //TODO
            return null;
        }
    }

    public static class HBaseEventDeserializer extends HBaseSourceDeserializer<HBaseEvent> {

        @Override
        public HBaseEvent deserialize(HBaseEvent event) {
            return event;
        }
    }

    public static class LongSerializer implements HBaseSinkSerializer<Long> {

        @Override
        public byte[] serializePayload(Long aLong) {
            return Bytes.toBytes(aLong);
        }

        @Override
        public byte[] serializeColumnFamily(Long aLong) {
            return Bytes.toBytes(Main.CF_Name);
        }

        @Override
        public byte[] serializeQualifier(Long aLong) {
            return Bytes.toBytes(0);
        }

        @Override
        public byte[] serializeRowKey(Long aLong) {
            return Bytes.toBytes(aLong);
        }
    }

}
