package org.apache.flink.connector.hbase.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    public static final Configuration HBASE_CONFIG = getDefaultConfig();

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
        private String[] tableNames;

        public Run(RunConfig config) {
            this.config = config;
        }

        public void run() {
            clearReplicationPeers();
            clearTables();
            setupFlinkEnvironment();
            createData();
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

        private void createTables() {
            tableNames = config.target.createTables(config);
        }

        private void setupFlinkEnvironment() {
        }

        private void createData() {

        }

        private void waitForTermination() {
        }

        private void retrieveResults() {

        }
    }

    public static abstract class BenchmarkGoal {
        public static class Througput extends BenchmarkGoal {}

        public static class Latency extends BenchmarkGoal {}
    }

    public static abstract class BenchmarkTarget {
        public abstract String[] createTables(RunConfig config);

        public static class Source extends BenchmarkTarget {
            @Override
            public String[] createTables(RunConfig config) {
                return null;
            }
        }
        public static class Sink extends BenchmarkTarget {
            @Override
            public String[] createTables(RunConfig config) {
                return null;
            }
        }
    }

    public static List<RunConfig> allRunConfigurations() {
        List<RunConfig> configs = new ArrayList<>();

        for (BenchmarkGoal goal : List.of(new BenchmarkGoal.Througput(), new BenchmarkGoal.Latency())) {
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
}
