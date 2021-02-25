package org.apache.flink.connector.hbase.benchmark;

import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.util.UUID;

public abstract class BenchmarkTarget {

    /** Create table name for this target, overridable for explicit naming patterns*/
    public String createTableName() {
        return "table-"+ UUID.randomUUID();
    }

    public abstract void augmentTableDescriptorForLatency(TableDescriptorBuilder basicTableDescriptor);

    public static class Source extends BenchmarkTarget {
        @Override
        public void augmentTableDescriptorForLatency(TableDescriptorBuilder basicTableDescriptor) {
            // Can be ignored, latency testing just needs the n column families
        }
    }
    public static class Sink extends BenchmarkTarget {
        public static final String CREATION_TIMESTAMP_CF = "creation_timestamp";
        @Override
        public void augmentTableDescriptorForLatency(TableDescriptorBuilder basicTableDescriptor) {
            basicTableDescriptor.setColumnFamily(ColumnFamilyDescriptorBuilder.of("CREATION_TIMESTAMP_CF"));
        }
    }

}
