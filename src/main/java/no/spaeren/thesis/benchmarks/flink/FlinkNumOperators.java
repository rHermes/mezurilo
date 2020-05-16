package no.spaeren.thesis.benchmarks.flink;

import no.spaeren.thesis.benchmarks.flink.helpers.MapIdentity;
import no.spaeren.thesis.benchmarks.flink.helpers.OnlyOne;
import no.spaeren.thesis.benchmarks.flink.sources.SeqGenSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "FlinkNumOperators", mixinStandardHelpOptions = true,
        description = "A number of operators benchmark")
public class FlinkNumOperators implements Callable<Void> {

    @CommandLine.Option(names = {"--from"}, defaultValue = "0")
    final Long from = 0L;

    @CommandLine.Option(names = {"--to"}, defaultValue = "100000000")
    final Long to = 100000000L;

    @CommandLine.Option(names = {"--event-time"})
    final Boolean useEventTime = false;

    @CommandLine.Option(names = {"--nodes"}, defaultValue = "1", description = "Number of operator nodes")
    final Long numberOfOperators = 1L;


    @Override
    public Void call() throws Exception {
        // Processing time
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (this.useEventTime) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }

        final DataStreamSource<Long> ds = env.addSource(new SeqGenSource(this.from, this.to));

        SingleOutputStreamOperator<Long> kov = ds;

        for (long i = 0; i < this.numberOfOperators; i++) {
            kov = kov.map(new MapIdentity<>());
        }



        kov.filter(new OnlyOne<>(this.to)).print("FlinkNumOperators");

        env.execute("FlinkNumOperators");
        return null;
    }
}
