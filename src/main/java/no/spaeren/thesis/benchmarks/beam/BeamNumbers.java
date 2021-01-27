package no.spaeren.thesis.benchmarks.beam;

import no.spaeren.thesis.benchmarks.beam.helpers.Printer;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import picocli.CommandLine;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "BeamNumbers", mixinStandardHelpOptions = true,
        description = "benchmarking the number of operators")
public class BeamNumbers implements Callable<Void> {

    @CommandLine.Option(names = {"--faster-copy"}, description = "Use the faster copy patch")
    final Boolean useFasterCopy = false;

    @CommandLine.Option(names = {"--kafka-broker"}, description = "What kafka broker to use")
    String kafkaBroker = "";

    @CommandLine.Option(names = {"--kafka-topic"}, description = "What is the name of the topic to connect to")
    String kafkaTopic = "";

    @Override
    public Void call() throws Exception {
        FlinkPipelineOptions options = PipelineOptionsFactory.create().as(FlinkPipelineOptions.class);
        options.setDisableMetrics(true);
        options.setRunner(FlinkRunner.class);
        options.setFasterCopy(useFasterCopy);
        options.setJobName("BeamNumbers");
        options.setStreaming(true);

        Pipeline p = Pipeline.create(options);

        PCollection<String> ins = p.apply(KafkaIO.<Long, String>read()
                .withBootstrapServers(kafkaBroker)
                .withTopic(kafkaTopic)
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withMaxReadTime(Duration.standardMinutes(10))
                .withoutMetadata()
        ).apply(Values.<String>create());



        PCollection<String> prins = ins.apply(Filter.by(
                input -> input != null && input.startsWith("print this")
        ));

        prins.apply(ParDo.of(new Printer<>("BeamNumbers: %s\n")));

        p.run().waitUntilFinish();
        return null;
    }
}
