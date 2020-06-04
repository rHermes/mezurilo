package no.spaeren.thesis.benchmarks.flink;

import no.spaeren.thesis.benchmarks.flink.sources.SeqGenSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import picocli.CommandLine;

import java.time.Duration;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "FlinkSimpleWindow", mixinStandardHelpOptions = true,
        description = "A simple benchmark for windowing on flink")
public class FlinkSimpleWindow implements Callable<Void> {

    @CommandLine.Option(names = {"--from"}, defaultValue = "0")
    final Long from = 0L;

    @CommandLine.Option(names = {"--to"}, defaultValue = "100000000")
    final Long to = 100000000L;

    @CommandLine.Option(names = {"--event-time"})
    final Boolean useEventTime = false;


    @CommandLine.Option(names = {"--window-duration"}, defaultValue = "PT5S", description = "The size of the tumbling window")
    final Duration windowDuration = Duration.ofSeconds(5);


    @Override
    public Void call() throws Exception {
        // Processing time
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (this.useEventTime) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }

        final DataStreamSource<Long> ds = env.addSource(new SeqGenSource(this.from, this.to));

        ds.map(x -> 1).timeWindowAll(Time.milliseconds(this.windowDuration.toMillis())).sum(0).print("FlinkSimpleWindow");

        env.execute("FlinkSimpleWindow");
        return null;
    }
}
