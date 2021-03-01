package org.apache.flink.connector.hbase.benchmark;

import org.apache.flink.connector.hbase.sink.HBaseSinkSerializer;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
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

    public abstract DataStream<StreamType> makeStreamFromSourceForThroughput(StreamExecutionEnvironment env, String tableName);

    public abstract DataStream<StreamType> makeStreamFromSourceForLatency(StreamExecutionEnvironment env, String tableName);

    public abstract void sinkForThroughput(DataStream<StreamType> stream, String tableName);

    public abstract void sinkForLatency(DataStream<StreamType> stream, String tableName);

    public abstract DataStream<StreamType> makeMapperForLatency(DataStream<StreamType> in, File resultFolder);

    public abstract void retrieveResultsForLatency(String tableName, File resultFolder);

    public abstract Class<StreamType> streamTypeClass();

    static class LongIterator implements Iterator<Long>, Serializable {

        private long i = 0;

        {
            System.out.println("Constructed LongIterator");
        }

        @Override
        public boolean hasNext() {
            return i <= 20000;
        }

        @Override
        public Long next() {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return i++;
        }
    }

    public static class HBaseEventDeserializer extends HBaseSourceDeserializer<HBaseEvent> {

        @Override
        public HBaseEvent deserialize(HBaseEvent event) {
            return event;
        }
    }

    public static class LongSerializer implements HBaseSinkSerializer<Long>, Serializable {

        @Override
        public byte[] serializePayload(Long aLong) {
            return Bytes.toBytes(aLong);
        }

        @Override
        public byte[] serializeColumnFamily(Long aLong) {
            return Bytes.toBytes(Main.CF_Name + 0);
        }

        @Override
        public byte[] serializeQualifier(Long aLong) {
            return Bytes.toBytes("0");
        }

        @Override
        public byte[] serializeRowKey(Long aLong) {
            return Bytes.toBytes(""+aLong);
        }
    }

}
