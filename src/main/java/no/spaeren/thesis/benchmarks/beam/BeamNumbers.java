package no.spaeren.thesis.benchmarks.beam;

import no.spaeren.thesis.benchmarks.beam.helpers.Printer;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
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

    @CommandLine.Option(names = {"--duration"}, defaultValue = "10", description = "How long the benchmark will run for, in minutes")
    final Long durationMin = 10L;

    @Override
    public Void call() throws Exception {
        ExperimentalOptions eopts = PipelineOptionsFactory.create().as(ExperimentalOptions.class);
        List<String> experiments = new ArrayList<>();
        experiments.add("use_deprecated_read");
        eopts.setExperiments(experiments);
        // FlinkPipelineOptions options = PipelineOptionsFactory.create().as(FlinkPipelineOptions.class);
        FlinkPipelineOptions options = eopts.as(FlinkPipelineOptions.class);
        //FlinkPipelineOptions options = PipelineOptionsFactory.create().as(FlinkPipelineOptions.class);
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
                .withMaxReadTime(Duration.standardMinutes(durationMin))
                .withoutMetadata()
        ).apply(Values.<String>create());



        PCollection<String> prins = ins.apply(Filter.by(
                input -> input != null && input.startsWith("print this")
        ));

        prins.apply(ParDo.of(new Printer<>("BeamNumbers: %s\n")));

        p.run(); // .waitUntilFinish();
        return null;
    }
}
