import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.flink.util.Collector;
import java.util.concurrent.atomic.AtomicInteger;

public class Main{
    public static long startTime = System.currentTimeMillis();
    public static AtomicInteger totalCount = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                // Print the total count every minute
                long currentTime = System.currentTimeMillis();
                System.out.println("Total count after " + (currentTime - startTime) / 1000 + " seconds: " + totalCount.get());
            }
        }, 0, 60000);
        DebeziumDeserializationSchema<String> deserializer =
                new JsonDebeziumDeserializationSchema();

        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("test")
                        .port(5432)
                        .database("postgres")
                        .schemaList("public")
                        .tableList("public.big_data")
                        .username("ankit")
                        .password("ankit123")
                        .slotName("flink")
                        .decodingPluginName("pgoutput") // use pgoutput for PostgreSQL 10+
                        .deserializer(deserializer).chunkKeyColumn("id")
                        .startupOptions(StartupOptions.initial()) // Explicitly start with a full snapshot
                        .splitSize(100000) // the split size of each snapshot split
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);

        env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(40)
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        totalCount.incrementAndGet();
//                        System.out.println("got element here: " + value);
                    }

                    @Override
                    public void close() throws Exception {
                        // This method will be called when the stream is finished.
                        System.out.println("Total subsequent records counted: " + totalCount.get());
                    }

                })
                .print();

        env.execute("Output Postgres Snapshot");
    }
}
