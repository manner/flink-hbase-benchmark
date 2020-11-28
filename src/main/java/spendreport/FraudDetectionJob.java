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

import org.apache.flink.connector.hbase.sink.HBaseMutationConverter;
import org.apache.flink.connector.hbase.sink.HBaseSinkFunction;
import org.apache.flink.connector.hbase.source.HBaseTableSource;
import org.apache.flink.connector.hbase.util.HBaseTableSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Skeleton code for the datastream walkthrough
 */
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort","2181");

        HBaseTableSource hBaseTableSource = new HBaseTableSource(config, "students");
//        HBaseTableSchema schema = new HBaseTableSchema();

//        schema.setRowKey("rowkey", byte[].class);
//        schema.addColumn("name", "last_name", byte[].class);

        hBaseTableSource.setRowKey("1", byte[].class);
        hBaseTableSource.addColumn("name", "last_name", byte[].class);

        DataStream<Row> rowDataStream = hBaseTableSource.getDataStream(env);
        rowDataStream.print();

//        HBaseUpsertTableSink sink = new HBaseUpsertTableSink(schema, HBaseWriteOptions.builder().setBufferFlushIntervalMillis(1000).build());



        HBaseMutationConverter<Alert> mutationConverter = new HBaseMutationConverter<Alert>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void open() {
            }

            @Override
            public Mutation convertToMutation(Alert alert) {
                Put put = new Put(Bytes.toBytes("10"));
                put.addImmutable(Bytes.toBytes("name"), Bytes.toBytes("first_name"), Bytes.toBytes("Mori"));

                return put;
            }
        };

        HBaseSinkFunction<Alert> hbaseSink = new HBaseSinkFunction<Alert>(
                "students", config, mutationConverter, 10000, 2, 1000);

        DataStream<Transaction> transactions = env
                .addSource(new TransactionSource())
                .name("transactions");

        DataStream<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts
                .addSink(hbaseSink)
                .name("send-alerts");

        env.execute("Fraud Detection");
    }
}

