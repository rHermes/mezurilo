package no.spaeren.thesis.benchmarks.flink;

import no.spaeren.thesis.benchmarks.flink.sources.SeqGenSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "FlinkSimple", mixinStandardHelpOptions = true, description = "A simple flink job")
public class FlinkSimple implements Callable<Void> {

    @CommandLine.Option(names = {"--from"}, defaultValue = "0")
    final Long from = 0L;

    @CommandLine.Option(names = {"--to"}, defaultValue = "1000")
    final Long to = 1000L;


    @Override
    public Void call() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<Long> ds = env.addSource(new SeqGenSource(this.from, this.to));

        ds.map(x -> x * 2).print("FlinkSimple");

        env.execute("FlinkSimple");
        return null;
    }
}
