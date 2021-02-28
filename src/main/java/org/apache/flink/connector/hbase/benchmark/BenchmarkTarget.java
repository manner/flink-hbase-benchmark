package org.apache.flink.connector.hbase.benchmark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    public DataStream<StreamType> makeMapperForThroughput(DataStream<StreamType> in, File resultFolder) {
        return in.map(new ThroughputMapper<>(resultFolder));
    }

    public abstract void retrieveResultsForLatency(String tableName);

    public abstract Class<StreamType> streamTypeClass();

    protected static class ThroughputMapper<T> implements MapFunction<T, T> {

        public static final int RESOLUTION = 100000; //TODO think bigger

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
            Main.runHBasePerformanceEvaluator(tableName, numberOfColumns, 1000000, 1);
        }

        @Override
        public DataStream<HBaseEvent> makeStreamFromSourceForThroughput(StreamExecutionEnvironment env, String tableName) {
            HBaseSource<HBaseEvent> source = new HBaseSource<>(
                    new HBaseEventDeserializer(),
                    tableName,
                    Main.HBASE_CONFIG);
            return env.fromSource(source, WatermarkStrategy.noWatermarks(), tableName).returns(HBaseEvent.class);
        }

        @Override
        public void sinkForThroughput(DataStream<HBaseEvent> stream, String tableName) {
            SinkFunction<HBaseEvent> sink = new DiscardingSink<>();
            stream.addSink(sink);
        }

        @Override
        public DataStream<HBaseEvent> makeStreamFromSourceForLatency(StreamExecutionEnvironment env, String tableName) {
            HBaseSource<HBaseEvent> source = new HBaseSource<>(
                    new HBaseEventDeserializer(),
                    tableName,
                    Main.HBASE_CONFIG);
            return env.fromSource(source, WatermarkStrategy.noWatermarks(), tableName).returns(HBaseEvent.class);
        }

        @Override
        public DataStream<HBaseEvent> makeMapperForLatency(DataStream<HBaseEvent> in, File resultFolder) {
            //TODO
            return null;
        }

        @Override
        public void retrieveResultsForLatency(String tableName) {
            //TODO
        }

        @Override
        public void sinkForLatency(DataStream<HBaseEvent> stream, String tableName) {
            SinkFunction<HBaseEvent> sink = new DiscardingSink<>();
            stream.addSink(sink);
        }

        @Override
        public Class<HBaseEvent> streamTypeClass() {
            return HBaseEvent.class;
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
        public DataStream<Long> makeStreamFromSourceForThroughput(StreamExecutionEnvironment env, String tableName) {
            NumberSequenceSource source = new NumberSequenceSource(0, 10000000);
            return env.fromSource(source, WatermarkStrategy.noWatermarks(), tableName);
        }

        @Override
        public void sinkForThroughput(DataStream<Long> stream, String tableName) {
            HBaseSink<Long> sink = new HBaseSink<>(tableName, new LongSerializer(), Main.HBASE_CONFIG);
            stream.sinkTo(sink);
        }

        @Override
        public DataStream<Long> makeStreamFromSourceForLatency(StreamExecutionEnvironment env, String tableName) {
            return env.fromCollection(new LongIterator(), Long.class);
        }

        @Override
        public DataStream<Long> makeMapperForLatency(DataStream<Long> in, File resultFolder) {
            return in.map((MapFunction<Long, Long>) value -> System.currentTimeMillis());
        }

        @Override
        public void retrieveResultsForLatency(String tableName) {
            try(
                    Connection connection = ConnectionFactory.createConnection(Main.HBASE_CONFIG)
            ) {
                Scan scan = new Scan().addColumn(Bytes.toBytes(Main.CF_Name+"0"), Bytes.toBytes("0"));
                ResultScanner scanner = connection.getTable(TableName.valueOf(tableName)).getScanner(scan);
                Result next;
                while((next = scanner.next()) != null) {
                    Cell cell = next.getColumnLatestCell(Bytes.toBytes(Main.CF_Name+"0"), Bytes.toBytes("0"));
                    long timeStamp = cell.getTimestamp();
                    long value = Bytes.toLong(CellUtil.cloneValue(cell));
                    System.out.println(timeStamp+","+value+","+(timeStamp-value));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void sinkForLatency(DataStream<Long> stream, String tableName) {
            HBaseSink<Long> sink = new HBaseSink<>(tableName, new LongSerializer(), Main.HBASE_CONFIG);
            stream.sinkTo(sink);
        }

        @Override
        public Class<Long> streamTypeClass() {
            return Long.class;
        }
    }

    private static class LongIterator implements Iterator<Long>, Serializable {

        private long i = 0;

        {
            System.out.println("Constructed LongIterator");
        }

        @Override
        public boolean hasNext() {
            return i <= 1000;
        }

        @Override
        public Long next() {
            try {
                Thread.sleep(10);
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
