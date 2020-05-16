package no.spaeren.thesis.benchmarks.flink;

import no.spaeren.thesis.benchmarks.flink.helpers.OnlyOne;
import no.spaeren.thesis.benchmarks.flink.sources.SeqGenSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "FlinkWatermark", mixinStandardHelpOptions = true,
        description = "A watermark benchmark")
public class FlinkWatermark implements Callable<Void> {

    @CommandLine.Option(names = {"--from"}, defaultValue = "0")
    final Long from = 0L;

    @CommandLine.Option(names = {"--to"}, defaultValue = "100000000")
    final Long to = 1000L;

    @CommandLine.Option(names = {"--event-time"})
    final Boolean useEventTime = false;


    @Override
    public Void call() throws Exception {
        System.out.printf("We will execute fw from %d to %d\n", this.from, this.to);

        // Processing time
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (this.useEventTime) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }
        final DataStreamSource<Long> ds = env.addSource(new SeqGenSource(this.from, this.to));
        ds.filter(new OnlyOne<>(this.to)).print("FlinkWatermark");

        env.execute("FlinkWatermark");
        return null;
    }
}
