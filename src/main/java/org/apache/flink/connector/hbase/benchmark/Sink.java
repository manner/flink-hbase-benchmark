package org.apache.flink.connector.hbase.benchmark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.connector.hbase.sink.HBaseSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;

public class Sink extends BenchmarkTarget<Long> {
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
    public DataStream<Long> makeStreamFromSourceForThroughput(
            StreamExecutionEnvironment env, String tableName) {
        NumberSequenceSource source = new NumberSequenceSource(0, 10000000);
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), tableName);
    }

    @Override
    public void sinkForThroughput(DataStream<Long> stream, String tableName) {
        HBaseSink<Long> sink = new HBaseSink<>(tableName, new LongSerializer(), Main.HBASE_CONFIG);
        stream.sinkTo(sink);
    }

    @Override
    public DataStream<Long> makeStreamFromSourceForLatency(
            StreamExecutionEnvironment env, String tableName) {
        return env.fromCollection(new LongIterator(), Long.class);
    }

    @Override
    public DataStream<Long> makeMapperForLatency(DataStream<Long> in, File resultFolder) {
        return in.map((MapFunction<Long, Long>) value -> System.currentTimeMillis());
    }

    @Override
    public void retrieveResultsForLatency(String tableName, File resultFolder) {
        try (Connection connection = ConnectionFactory.createConnection(Main.HBASE_CONFIG)) {
            CSVWriter csvWriter =
                    new CSVWriter(
                            resultFolder, new String[] {"hbasetimestamp", "flinktimestamp", "difference"});
            Scan scan = new Scan().addColumn(Bytes.toBytes(Main.CF_Name + "0"), Bytes.toBytes("0"));
            ResultScanner scanner = connection.getTable(TableName.valueOf(tableName)).getScanner(scan);
            Result next;
            while ((next = scanner.next()) != null) {
                Cell cell = next.getColumnLatestCell(Bytes.toBytes(Main.CF_Name + "0"), Bytes.toBytes("0"));
                long timeStamp = cell.getTimestamp();
                long value = Bytes.toLong(CellUtil.cloneValue(cell));
                csvWriter.writeRow("" + timeStamp, "" + value, "" + (timeStamp - value));
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
