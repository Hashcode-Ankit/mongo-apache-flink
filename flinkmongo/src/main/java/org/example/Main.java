package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import java.util.Timer;
import java.util.TimerTask;
import java.io.IOException;
import java.time.Instant;

public class Main{
    public static long startTime = System.currentTimeMillis();
    public static int totalCount = 0;
    public static void main(String[] args) throws Exception {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                // Print the total count every minute
                long currentTime = System.currentTimeMillis();
                System.out.println("Total count after " + (currentTime - startTime) / 1000 + " seconds: " + totalCount);
            }
        }, 0, 60000);
        MongoDBSource<String> mongoSource =
                MongoDBSource.<String>builder()
                        .hosts("tiharjail")
                        .databaseList("twitter_data") // set captured database, support regex
                        .collectionList("twitter_data.tweets") //set captured collections, support regex
                        .username("dekh dekh dekh")
                        .password("jayada hoshiyar hora")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(60);

        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 2
//        env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDBIncrementalSource")
//                .setParallelism(2)
//                .print()
//                .setParallelism(1);
        env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDBIncrementalSource")
                .setParallelism(60)
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        totalCount++; // Increment count for subsequent records
                    }

                    @Override
                    public void close() throws Exception {
                        // This method will be called when the stream is finished.
                        System.out.println("Total subsequent records counted: " + totalCount);
                    }

                }).setParallelism(60); // Ensure only one instance for printing

        env.execute("Print MongoDB Snapshot + Change Stream");

    }
}
