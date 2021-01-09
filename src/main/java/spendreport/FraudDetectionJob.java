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
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton code for the datastream walkthrough
 */
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        DataStream<String> names = env.readTextFile("file:///Users/moritzmanner/Projects/frauddetection/src/main/java/spendreport/names-10.txt");
//        names.print();
        NumberSequenceSource numberSequenceSource = new NumberSequenceSource(1, 10);

        DataStream<Long> nums = env.fromSource(
                numberSequenceSource,
                WatermarkStrategy.noWatermarks(),
                "nums");
        nums.print();
//        TypeInformation<String> typeInfo = TypeInformation.of(String.class);
//        HbaseSource<String> source = new HbaseSource<>(Boundedness.BOUNDED);
//        source.getBoundedness();
//        DataStream<String> stream = env.fromSource(
//                source,
//                WatermarkStrategy.noWatermarks(),
//                "HBaseSource",
//                typeInfo
//        );
//        stream.print();


        //
//        DataStream<String> parsed = names.map(
//                new MapFunction<String, String>() {
//                    @Override
//                    public String map(String value) throws Exception {
//                        int len = value.length();
//                        return value + " " + len;
//                    }
//                }
//        );
//        parsed.writeAsText("file:///Users/moritzmanner/Projects/frauddetection/src/main/java/spendreport/parsed.txt").setParallelism(1);;
//        parsed.print();

        env.execute("Word length mapper");
    }
}

