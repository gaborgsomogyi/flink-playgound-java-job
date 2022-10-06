/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.flink;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

import com.apple.flink.avro.Data;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkPlaygroundJavaJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkPlaygroundJavaJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting playground job");

        Options options = new Options();

        CommandLineParser parser = new DefaultParser();
        try {
            LOG.info("Parsing command line");
            CommandLine commandLine = parser.parse(options, args);

            mainWithStreamingAPI(commandLine);
            // mainWithTableAPI(commandLine);
        } catch (ParseException e) {
            LOG.error("Unable to parse command line options: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(FlinkPlaygroundJavaJob.class.getCanonicalName(), options);
        }

        LOG.info("Existing playground job");
    }

    static class AsyncEnrichmentRequest extends RichAsyncFunction<Data, Data> {
        private final Random random = new Random();

        @Override
        public void asyncInvoke(Data data, final ResultFuture<Data> resultFuture) {
            CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    int sleepMs = 1000 + random.nextInt(1000);
                                    LOG.info("Data: {} sleep({})", data, sleepMs);
                                    Thread.sleep(sleepMs);
                                    data.setSleepMs(sleepMs);
                                    LOG.info("Data: {} complete", data);
                                    return data;
                                } catch (InterruptedException e) {
                                    return null;
                                }
                            })
                    .thenAccept((Data d) -> resultFuture.complete(Collections.singleton(d)));
        }
    }

    private static void mainWithStreamingAPI(CommandLine commandLine) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream =
                env.addSource(
                                new DataGeneratorSource<>(
                                        RandomGenerator.stringGenerator(8), 4, null))
                        .returns(String.class)
                        .uid("datagen-source-uid")
                        .name("datagen-source");

        DataStream<Data> dataStream = stream.map(v -> new Data(v, 0)).returns(Data.class);
        AsyncDataStream.unorderedWait(
                        dataStream, new AsyncEnrichmentRequest(), 3, TimeUnit.SECONDS, 256)
                .uid("enrichment-uid")
                .name("enrichment")
                .print()
                .uid("print-sink-uid")
                .name("print-sink");

        env.execute("playground");
    }

    private static void mainWithTableAPI(CommandLine commandLine) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        LOG.info("Creating table environment");
        TableEnvironment tEnv = TableEnvironment.create(settings);

        LOG.info("Creating datagen table");
        tEnv.createTable(
                "DatagenTable",
                TableDescriptor.forConnector("datagen")
                        .schema(Schema.newBuilder().column("f0", DataTypes.STRING()).build())
                        .option("fields.f0.kind", "random")
                        .option("fields.f0.length", "8")
                        .option(DataGenConnectorOptions.ROWS_PER_SECOND, 1L)
                        .build());

        Table datagenTable = tEnv.from("DatagenTable").select($("f0"));

        LOG.info("Creating print table");
        final String printTableName = "PrintTable";
        tEnv.createTable(
                printTableName,
                TableDescriptor.forConnector("print")
                        .schema(Schema.newBuilder().column("f0", DataTypes.STRING()).build())
                        .build());

        LOG.info("Executing insert");
        datagenTable.executeInsert(printTableName);
    }
}
