/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pepkorit.cloud.solutions.realtimeredisupdate.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.flogger.FluentLogger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.redis.RedisIO;
import org.apache.beam.sdk.io.redis.RedisIO.Write.Method;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.joda.time.Duration;

/**
 * Realtime Dataflow pipeline to extract experiment metrics from Log Events
 * published on Pub/Sub.
 */
public final class RealTimeRedisUpdatePipeline {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    /**
     * Parses the command line arguments and runs the pipeline.
     */
    public static void main(String[] args) {
        RealTimeRedisUpdatePipelineOptions options = extractPipelineOptions(args);

        Pipeline pipeline = Pipeline.create(options);

        String project = options.getBigQueryInputProject();
        String dataset = options.getBigQueryInputDataset();
        String table = options.getBigQueryInputTable();
        Long lowWatermark = Long.valueOf(options.getLowWaterMark());

    
        String query = String.format("SELECT sku_id, sku_code FROM `%s.%s.%s`  where cpy_id <> 2;",
                project, dataset, "central_sku_vw"); 

         final PCollectionView<Map<String, String>> skus = pipeline
                // Emitted long data trigger this batch read BigQuery client job.
                .apply(String.format("Read big query central sku table.  Updating every %s hours", 10),
                        GenerateSequence.from(0).withRate(1, Duration.standardHours(10)))
                .apply(
                        Window.<Long>into(new GlobalWindows())
                                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                                .discardingFiredPanes()) // Caching results as Map.

                .apply(new ReadSlowChangingBigQueryTable("Read BigQuery SKU Table", query,
                        "sku_id",
                        "sku_code"))
                // Caching results as Map.
                .apply("Sku View As Map", View.asMap());

        PCollection<BranchCompanySkuTransactionValue> allDebeziumEvents
                = pipeline
                .apply("Read PubSub Events",
                        PubsubIO.readStrings().fromTopic(options.getInputTopic()))
                .apply("Parse Message JSON",
                        ParDo.of(new ParseMessageAsLogElement()));

        PCollection<KV<String, String>> filteredDebeziumEvents = allDebeziumEvents
                .apply("Filtering out messages not related to transactions",Filter.by(new SerializableFunction<BranchCompanySkuTransactionValue, Boolean>() {
                    @Override
                    public Boolean apply(BranchCompanySkuTransactionValue input) {
                        return input.getTable().equals("transaction_detail_item");
                    }
                }))
                .apply("Filtering out messages not which are not create events",Filter.by(new SerializableFunction<BranchCompanySkuTransactionValue, Boolean>() {
                    @Override
                    public Boolean apply(BranchCompanySkuTransactionValue input) {
                        return input.getOp().equals("c");
                    }
                }))
                .apply("Create key value pair",ParDo.of(
                                new DoFn<BranchCompanySkuTransactionValue, KV<String, String>>() {
                                    @ProcessElement
                                    public void getKeyValue(ProcessContext context) throws JsonProcessingException {
                                        BranchCompanySkuTransactionValue updateValue = context.element();
                                        context.output(
                                                KV.of(updateValue.getTransactionId(),
                                                        BranchCompanySkuTransactionValue.getJsonStringFromObject(updateValue))
                                        );
                                    }
                                }
                        )
                )
                ;

        PCollection<BranchCompanySkuTransactionValue> dedupedEvents = filteredDebeziumEvents
                .apply("Deduplicate values",Deduplicate.<String,String>keyedValues().withDuration(Duration.standardHours(10))
                )
                .apply("Convert value back to object",ParDo.of(
                                new DoFn<KV<String, String>, BranchCompanySkuTransactionValue>() {
                                    @ProcessElement
                                    public void getKeyValue(ProcessContext context) throws JsonProcessingException {
                                        String stringObject = context.element().getValue();
                                        context.output(
                                                        BranchCompanySkuTransactionValue.getObjectFromJsonString(stringObject)
                                        );
                                    }
                                }
                        )

                )
                ;

        

       PCollection<BranchCompanySkuTransactionValue> sku_output = dedupedEvents.apply(
                ParDo
                        .of(new AddSkuDataToMessage(skus))
                        .withSideInput("skus", skus)
        ); 



        pipeline.run();
    }

    /**
     * Parse Pipeline options from command line arguments.
     */
    private static RealTimeRedisUpdatePipelineOptions extractPipelineOptions(String[] args) {
        RealTimeRedisUpdatePipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(RealTimeRedisUpdatePipelineOptions.class);

        return options;
    }
}