package org.apache.flink.connector.hbase.benchmark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.hbase.sink.HBaseSink;
import org.apache.flink.connector.hbase.sink.HBaseSinkSerializer;
import org.apache.flink.connector.hbase.source.HBaseSource;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    public abstract DataStream<StreamType> makeStreamFromSourceForThroughput(StreamExecutionEnvironment env, String id);

    public abstract DataStream<StreamType> makeStreamFromSourceForLatency(StreamExecutionEnvironment env, String id);

    public abstract void sinkForThroughput(DataStream<StreamType> stream);

    public abstract void sinkForLatency(DataStream<StreamType> stream);

    public abstract DataStream<StreamType> makeMapperForLatency(DataStream<StreamType> in, File resultFolder);

    public DataStream<StreamType> makeMapperForThroughput(DataStream<StreamType> in, File resultFolder) {
        return in.map(new ThroughputMapper<>(resultFolder, in.getType())).returns(in.getType());
    }

    protected static class ThroughputMapper<T> implements MapFunction<T, T> , ResultTypeQueryable<T> {

        public static final int RESOLUTION = 1000; //TODO think bigger

        private int count = 0;
        private long lastTimeStamp = -1;
        private final String resultPath;
        private final TypeInformation<T> typeInfo;

        public ThroughputMapper(File resultFolder, TypeInformation<T> typeInfo) {
            this.resultPath = resultFolder.toPath().resolve(UUID.randomUUID().toString()+".csv").toAbsolutePath().toString();
            this.typeInfo = typeInfo;
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

        @Override
        public TypeInformation<T> getProducedType() {
            return typeInfo;
        }
    }

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
        public DataStream<HBaseEvent> makeStreamFromSourceForThroughput(StreamExecutionEnvironment env, String id) {
            HBaseSource<HBaseEvent> source = new HBaseSource<>(
                    Boundedness.CONTINUOUS_UNBOUNDED,
                    new HBaseEventDeserializer(),
                    id,
                    Main.HBASE_CONFIG);
            return env.fromSource(source, WatermarkStrategy.noWatermarks(), id);
        }

        @Override
        public void sinkForThroughput(DataStream<HBaseEvent> stream) {
            SinkFunction<HBaseEvent> sink = new DiscardingSink<>();
            stream.addSink(sink);
        }

        @Override
        public DataStream<HBaseEvent> makeStreamFromSourceForLatency(StreamExecutionEnvironment env, String id) {
            HBaseSource<HBaseEvent> source = new HBaseSource<>(
                    Boundedness.CONTINUOUS_UNBOUNDED,
                    new HBaseEventDeserializer(),
                    id,
                    Main.HBASE_CONFIG);
            return env.fromSource(source, WatermarkStrategy.noWatermarks(), id).returns(HBaseEvent.class);
        }

        @Override
        public DataStream<HBaseEvent> makeMapperForLatency(DataStream<HBaseEvent> in, File resultFolder) {
            //TODO
            return null;
        }

        @Override
        public void sinkForLatency(DataStream<HBaseEvent> stream) {
            SinkFunction<HBaseEvent> sink = new DiscardingSink<>();
            stream.addSink(sink);
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
        public DataStream<Long> makeStreamFromSourceForThroughput(StreamExecutionEnvironment env, String id) {
            NumberSequenceSource source = new NumberSequenceSource(0, 10000);
            return env.fromSource(source, WatermarkStrategy.noWatermarks(), id);
        }

        @Override
        public void sinkForThroughput(DataStream<Long> stream) {
            HBaseSink<Long> sink = new HBaseSink<>("tableName", new LongSerializer(), Main.HBASE_CONFIG);
            stream.sinkTo(sink);
        }

        @Override
        public DataStream<Long> makeStreamFromSourceForLatency(StreamExecutionEnvironment env, String id) {
            //TODO make periodic return new NumberSequenceSource(0, 10000);
            return null;
        }

        @Override
        public DataStream<Long> makeMapperForLatency(DataStream<Long> in, File resultFolder) {
            return in.map((MapFunction<Long, Long>) value -> System.currentTimeMillis());
        }

        @Override
        public void sinkForLatency(DataStream<Long> stream) {
            HBaseSink<Long> sink = new HBaseSink<>("tableName", new LongSerializer(), Main.HBASE_CONFIG);
            stream.sinkTo(sink);
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
