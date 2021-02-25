package org.apache.flink.connector.hbase.benchmark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static final Configuration HBASE_CONFIG = getDefaultConfig();
    public static final String CF_Name = "info";

    public static void main(String[] args) {
        for (RunConfig runConfig : allRunConfigurations()) {
            new Run(runConfig).run();
        }
    }

    public static class RunConfig {
        public final int numberOfColumns;
        public final int parallelism;
        public final BenchmarkGoal goal;
        public final BenchmarkTarget target;

        public RunConfig(int numberOfColumns, int parallelism, BenchmarkGoal goal, BenchmarkTarget target) {
            this.numberOfColumns = numberOfColumns;
            this.parallelism = parallelism;
            this.goal = goal;
            this.target = target;
        }
    }

    public static class Run {
        public final RunConfig config;
        private String tableName;

        public Run(RunConfig config) {
            this.config = config;
        }

        public void run() {
            clearReplicationPeers();
            clearTables();
            createTable();
            setupFlinkEnvironment();
            createData(tableName, config.numberOfColumns, 100000, 1);
            waitForTermination();
            retrieveResults();
        }


        private void clearReplicationPeers() {
            System.out.println("Clearing replication peers ...");
            try (Admin admin = ConnectionFactory.createConnection(HBASE_CONFIG).getAdmin()) {
                for (ReplicationPeerDescription desc : admin.listReplicationPeers()) {
                    admin.removeReplicationPeer(desc.getPeerId());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void clearTables() {
            System.out.println("Clearing tables ...");
            try (Admin admin = ConnectionFactory.createConnection(HBASE_CONFIG).getAdmin()) {
                for (TableDescriptor desc : admin.listTableDescriptors()) {
                    admin.deleteTable(desc.getTableName());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void createTable() {
            tableName = config.target.createTableName();
            TableDescriptorBuilder basicTableDescriptor = basicTableDescriptor(tableName, config.numberOfColumns);
            config.goal.augmentTableDescriptor(basicTableDescriptor, config.target);
            Main.createTable(basicTableDescriptor);
        }

        private void setupFlinkEnvironment() {
            StreamExecutionEnvironment env = new StreamExecutionEnvironment();
            NumberSequenceSource sequenceSource = new NumberSequenceSource(0, 10);
            DataStream<Long> stream = env.fromSource(sequenceSource, WatermarkStrategy.noWatermarks(), "sequence");
            KeyedStream<Long, Boolean> keyedStream = stream.keyBy(n -> n % 2 == 0);
            stream = keyedStream.reduce((ReduceFunction<Long>) (value1, value2) -> value1 + value2);
            stream.print();
        }

        private void createData(String tableName, int noOfFamilies, int noOfRows, int noOfWriters) {
            try {
                Runtime.getRuntime()
                        .exec(String.format("hbase pe --table=%s --families=%d --rows=%d --nomapred sequentialWrite %d",
                                tableName, noOfFamilies, noOfRows, noOfWriters));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void waitForTermination() {
        }

        private void retrieveResults() {

        }
    }

    public static List<RunConfig> allRunConfigurations() {
        List<RunConfig> configs = new ArrayList<>();

        for (BenchmarkGoal goal : List.of(new BenchmarkGoal.Throughput(), new BenchmarkGoal.Latency())) {
            for(BenchmarkTarget target : List.of(new BenchmarkTarget.Source(), new BenchmarkTarget.Sink())) {
                for (int cols : List.of(1, 2, 10)) {
                    for (int parallelism : List.of(1, 2, 8)) {
                        configs.add(new RunConfig(cols, parallelism, goal, target));
                    }
                }
            }
        }
        return configs;
    }


    private static Configuration getDefaultConfig() {
        Configuration configuration = HBaseConfiguration.create();

        configuration.setInt("replication.stats.thread.period.seconds", 5);
        configuration.setLong("replication.sleep.before.failover", 2000);
        configuration.setInt("replication.source.maxretriesmultiplier", 10);
        configuration.setBoolean("hbase.replication", true);

        return configuration;
    }

    private static TableDescriptorBuilder basicTableDescriptor(String tableNameString, int numColumnFamilies) {
        TableName tableName = TableName.valueOf(tableNameString);
        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
        for (int i = 0; i < numColumnFamilies; i++) {
            ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(CF_Name + i));
            cfBuilder.setScope(1);
            tableBuilder.setColumnFamily(cfBuilder.build());
        }
        return tableBuilder;
    }

    private static void createTable(TableDescriptorBuilder tableBuilder) {
        try(Admin admin = ConnectionFactory.createConnection(HBASE_CONFIG).getAdmin()) {
            admin.createTable(tableBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
