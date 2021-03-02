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

package org.apache.flink.connector.hbase.benchmark;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;


/**
 * Skeleton code for the datastream walkthrough
 */
public class RetrieveHBaseThroughput {

    public static void main(String[] args) {

        String tableName = "";
        try (Connection connection = ConnectionFactory.createConnection(Main.HBASE_CONFIG)) {
            CSVWriter csvWriter =
                    new CSVWriter(
                            new File("results"), new String[]{"rowId", "HbaseTimestamp", "diff", "size"});
            Table table = connection.getTable(TableName.valueOf(tableName));
            long lastTimestamp = 0;
            for (int i = 0; i < 100_000_001; i += 1_000_000) {
                String paddedRowKey = String.format("%026d", i);
                Get get = new Get(Bytes.toBytes(paddedRowKey));
                Result result = table.get(get);
                Cell cell = result.getColumnLatestCell(Bytes.toBytes(Main.CF_Name + "0"), Bytes.toBytes("0"));
                long timeStamp = cell.getTimestamp();
                long resultSize = Result.getTotalSizeOfCells(result);
                if (lastTimestamp > 0) {
                    csvWriter.writeRow("" + i, "" + timeStamp, "" + (timeStamp - lastTimestamp), resultSize + "");
                }
                lastTimestamp = timeStamp;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

