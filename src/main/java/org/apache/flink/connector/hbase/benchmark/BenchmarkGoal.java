package org.apache.flink.connector.hbase.benchmark;

import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;

public abstract class BenchmarkGoal {

    public abstract void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target);
    public abstract void makeData(String tableName, int numberOfColumns);

    public static class Throughput extends BenchmarkGoal {
        @Override
        public void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target) {
            // Ignore, basic tables are enough for throughput testing
        }

        @Override
        public void makeData(String tableName, int numberOfColumns) {
            runHBasePerformanceEvaluator(tableName, numberOfColumns, 100000, 1);
        }

        public void runHBasePerformanceEvaluator(String tableName, int noOfFamilies, int noOfRows, int noOfWriters) {
            try {
                Runtime.getRuntime()
                        .exec(String.format("hbase pe --table=%s --families=%d --rows=%d --nomapred sequentialWrite %d",
                                tableName, noOfFamilies, noOfRows, noOfWriters));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Latency extends BenchmarkGoal {

        @Override
        public void makeData(String tableName, int numberOfColumns) {
            //TODO
        }

        @Override
        public void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target) {
            target.augmentTableDescriptorForLatency(basicTableDescriptor);
        }
    }

}
