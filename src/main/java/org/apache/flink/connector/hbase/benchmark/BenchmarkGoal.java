package org.apache.flink.connector.hbase.benchmark;

import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;

public abstract class BenchmarkGoal {

    public abstract void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target);
    public abstract void makeData(String tableName, int numberOfColumns, BenchmarkTarget target);

    public static class Throughput extends BenchmarkGoal {
        @Override
        public void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target) {
            // Ignore, basic tables are enough for throughput testing
        }

        @Override
        public void makeData(String tableName, int numberOfColumns, BenchmarkTarget target) {
            target.makeDataForThroughput(tableName, numberOfColumns);
        }
    }

    public static class Latency extends BenchmarkGoal {

        @Override
        public void makeData(String tableName, int numberOfColumns, BenchmarkTarget target) {
            //TODO
        }

        @Override
        public void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target) {
            target.augmentTableDescriptorForLatency(basicTableDescriptor);
        }
    }

}
