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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple2;
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

    public static final String COLUMN_FAMILY_NAME = "info0";
    public static final String DEFAULT_TABLE_NAME = "latency";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = HBaseConfiguration.create();


        configuration.setInt("replication.stats.thread.period.seconds", 5);
        configuration.setLong("replication.sleep.before.failover", 2000);
        configuration.setInt("replication.source.maxretriesmultiplier", 10);
        configuration.setBoolean("hbase.replication", true);

        System.out.println(configuration.get("hbase.zookeeper.quorum"));
        System.out.println(configuration.get("hbase.zookeeper.property.clientPort"));
        System.out.println(configuration.get("hbase.master.info.port"));
        System.out.println(configuration.get("hbase.master.port"));
        System.out.println(configuration.get("hbase.master.info.port"));
        System.out.println(configuration.get("hbase.master.info.port"));

        clearPeers(configuration);
        createSchema(configuration, DEFAULT_TABLE_NAME + "-in");
        createSchema(configuration, DEFAULT_TABLE_NAME + "-out");

        /* Need external process that inserts elements in HBase */
//        2021 - 02 - 16 T18:
//        32:04.139
//        2021 - 02 - 16 T18:
//        32:02.727
//
//        2021 - 02 - 16 T18:
//        35:14.146
//        2021 - 02 - 16 T18:
//        35:13.970
        HBaseSource<Tuple2<String, String>> source =
                new HBaseSource<>(
                        Boundedness.CONTINUOUS_UNBOUNDED,
                        new HBaseStringDeserializationSchema(),
                        DEFAULT_TABLE_NAME + "-in",
                        configuration);

        DataStream<Tuple2<String, String>> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "HBaseSource", new HBaseStringDeserializationSchema().getProducedType());

        stream.map((MapFunction<Tuple2<String, String>, Tuple2<String, String>>) value -> new Tuple2<>(value.f0, "A_" + value.f1))
                .returns(Types.TUPLE(Types.STRING, Types.STRING));

        HBaseSink<Tuple2<String, String>> sink =
                new HBaseSink<>(
                        DEFAULT_TABLE_NAME + "-out",
                        new HBaseStringSerializationSchema(),
                        configuration);

        stream.sinkTo(sink);

        env.execute("HBaseBenchmark");
    }

    public static void createSchema(Configuration hbaseConf, String tableName) throws IOException {
        Admin admin = ConnectionFactory.createConnection(hbaseConf).getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            HColumnDescriptor infoCf = new HColumnDescriptor(COLUMN_FAMILY_NAME);
            infoCf.setScope(1);
            tableDescriptor.addFamily(infoCf);

            admin.createTable(tableDescriptor);
        }
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
            extends HBaseSourceDeserializer<Tuple2<String, String>> {

        public Tuple2<String, String> deserialize(HBaseEvent event) {
            return new Tuple2<>(event.getRowId(), new String(event.getPayload()));
        }
    }

    /**
     * HBaseStringSerializationSchema.
     */
    public static class HBaseStringSerializationSchema
            implements HBaseSinkSerializer<Tuple2<String, String>>, Serializable {

        @Override
        public byte[] serializePayload(Tuple2<String, String> event) {
            return Bytes.toBytes(event.f1);
        }

        @Override
        public byte[] serializeColumnFamily(Tuple2<String, String> event) {
            return Bytes.toBytes(COLUMN_FAMILY_NAME);
        }

        @Override
        public byte[] serializeQualifier(Tuple2<String, String> event) {
            return Bytes.toBytes("0");
        }

        @Override
        public byte[] serializeRowKey(Tuple2<String, String> event) {
            return Bytes.toBytes(event.f0);
        }
    }
}

