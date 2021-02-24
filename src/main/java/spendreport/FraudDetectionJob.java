/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.hbase.sink.HBaseSink;
import org.apache.flink.connector.hbase.sink.HBaseSinkSerializer;
import org.apache.flink.connector.hbase.source.HBaseSource;
import org.apache.flink.connector.hbase.source.reader.HBaseEvent;
import org.apache.flink.connector.hbase.source.reader.HBaseSourceDeserializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;


/**
 * Skeleton code for the datastream walkthrough
 */
public class FraudDetectionJob {

    public static final String COLUMN_FAMILY_NAME = "info";
    public static final String DEFAULT_TABLE_NAME = "latency";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = HBaseConfiguration.create();


        configuration.setInt("replication.stats.thread.period.seconds", 5);
        configuration.setLong("replication.sleep.before.failover", 2000);
        configuration.setInt("replication.source.maxretriesmultiplier", 10);
        configuration.setBoolean("hbase.replication", true);

        clearPeers(configuration);

        createSchema(configuration, DEFAULT_TABLE_NAME + "-in");
        createSchema(configuration, DEFAULT_TABLE_NAME + "-out");

        HBaseSourceDeserializer<Tuple3<String, String, String>> sourceDeserializer = new HBaseStringDeserializationSchema();

        HBaseSource<Tuple3<String, String, String>> source =
                new HBaseSource<>(
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        sourceDeserializer,
                        DEFAULT_TABLE_NAME + "-in",
                        configuration);

        DataStream<Tuple3<String, String, String>> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "HBaseSource", sourceDeserializer.getProducedType());

        stream = stream.map((MapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>>) value ->
                Tuple3.of(value.f0, value.f1, "TEST! " + value.f2));

        HBaseSink<Tuple3<String, String, String>> sink =
                new HBaseSink<>(
                        DEFAULT_TABLE_NAME + "-out",
                        new HBaseStringSerializationSchema(),
                        configuration);

        stream.sinkTo(sink);

        env.execute("HBaseBenchmark");
    }

    public static void createSchema(Configuration hbaseConf, String tableName) throws IOException {
        Admin admin = ConnectionFactory.createConnection(hbaseConf).getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (int i = 0; i < 2; i++) {
            HColumnDescriptor infoCf = new HColumnDescriptor(COLUMN_FAMILY_NAME + i);
            infoCf.setScope(1);
            tableDescriptor.addFamily(infoCf);
        }
        admin.createTable(tableDescriptor);

        admin.close();
    }

    public static void clearPeers(Configuration config) {

        try (Admin admin = ConnectionFactory.createConnection(config).getAdmin()) {
            for (ReplicationPeerDescription desc : admin.listReplicationPeers()) {
                System.out.println("==== " + desc.getPeerId() + " ====");
                System.out.println(desc);
                admin.removeReplicationPeer(desc.getPeerId());
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class HBaseStringDeserializationSchema
            extends HBaseSourceDeserializer<Tuple3<String, String, String>> {

        public Tuple3<String, String, String> deserialize(HBaseEvent event) {
            return Tuple3.of(event.getRowId(), event.getCf(), new String(event.getPayload()));
        }
    }

    /**
     * HBaseStringSerializationSchema.
     */
    public static class HBaseStringSerializationSchema
            implements HBaseSinkSerializer<Tuple3<String, String, String>>, Serializable {

        @Override
        public byte[] serializePayload(Tuple3<String, String, String> event) {
            return Bytes.toBytes(event.f2);
        }

        @Override
        public byte[] serializeColumnFamily(Tuple3<String, String, String> event) {
            return Bytes.toBytes(event.f1);
        }

        @Override
        public byte[] serializeQualifier(Tuple3<String, String, String> event) {
            return Bytes.toBytes("0");
        }

        @Override
        public byte[] serializeRowKey(Tuple3<String, String, String> event) {
            return Bytes.toBytes(event.f0);
        }
    }
}

