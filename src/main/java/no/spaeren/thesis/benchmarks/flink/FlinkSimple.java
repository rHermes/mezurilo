package no.spaeren.thesis.benchmarks.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkSimple {

    public void execute(String... args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        final DataStreamSource<Long> ds = env.generateSequence(0, 1000);


        ds.map(x -> x * 2).print("FlinkSimple");

        env.execute("FlinkSimple");
    }
}
