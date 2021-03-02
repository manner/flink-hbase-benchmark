package org.apache.flink.connector.hbase.benchmark;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.hbase.source.HBaseSource;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;

public class Source extends BenchmarkTarget<HBaseEvent> {
    @Override
    public void augmentTableDescriptorForLatency(TableDescriptorBuilder basicTableDescriptor) {
        // Can be ignored, latency testing just needs the n column families
    }

    @Override
    public void makeDataForLatency(String tableName, int numberOfColumns) {
        long count = 0;
        try {
            Connection connection = ConnectionFactory.createConnection(Main.HBASE_CONFIG);
            Table table = connection.getTable(TableName.valueOf(tableName));
            while (count < 10000) {
                Put put = new Put(Bytes.toBytes(count++));
                put.addColumn(
                        Bytes.toBytes(Main.CF_Name + "0"),
                        Bytes.toBytes("0"),
                        Bytes.toBytes("MCFOOBARINDAHOUSE"));
                table.put(put);
                Thread.sleep(10);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void makeDataForThroughput(String tableName, int numberOfColumns) {
        Main.runHBasePerformanceEvaluator(tableName, numberOfColumns, 100_000_001, 1);
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
        CSVWriter csvWriter =
                new CSVWriter(
                        resultFolder, new String[] {"hbasetimestamp", "flinktimestamp", "difference"});
        return in.map((MapFunction<HBaseEvent, HBaseEvent>) value -> {
            long hbaseTimestamp = value.getTimestamp();
            long flinkTimestamp = System.currentTimeMillis();

            long diff = flinkTimestamp - hbaseTimestamp;
            csvWriter.writeRow("" + hbaseTimestamp, "" + flinkTimestamp, "" + diff);
            return value;
        });
    }

    @Override
    public void retrieveResultsForLatency(String tableName, File resultFolder) {
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