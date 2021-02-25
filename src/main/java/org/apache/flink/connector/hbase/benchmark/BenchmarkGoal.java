package org.apache.flink.connector.hbase.benchmark;

import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

public abstract class BenchmarkGoal {

    public abstract void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target);

    public static class Throughput extends BenchmarkGoal {
        @Override
        public void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target) {
            // Ignore, basic tables are enough for throughput testing
        }
    }

    public static class Latency extends BenchmarkGoal {
        @Override
        public void augmentTableDescriptor(TableDescriptorBuilder basicTableDescriptor, BenchmarkTarget target) {
            target.augmentTableDescriptorForLatency(basicTableDescriptor);
        }
    }

}
