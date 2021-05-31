package no.spaeren.thesis.benchmarks.beam;

import no.spaeren.thesis.benchmarks.beam.helpers.CountSource;
import no.spaeren.thesis.benchmarks.beam.helpers.Printer;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import picocli.CommandLine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "BeamSimpleWindow", mixinStandardHelpOptions = true,
        description = "A simple benchmark for windowing on beam")
public class BeamSimpleWindow implements Callable<Void> {

    @CommandLine.Option(names = {"--from"}, defaultValue = "0")
    final Long from = 0L;

    @CommandLine.Option(names = {"--to"}, defaultValue = "100000000")
    final Long to = 100000000L;

    @CommandLine.Option(names = {"--window-duration"}, defaultValue = "PT5S", description = "The size of the tumbling window")
    final Duration windowDuration = Duration.ofSeconds(5);

    @CommandLine.Option(names = {"--name"}, defaultValue = "BeamSimpleWindow", description = "The name used in logging")
    String name = "BeamSimpleWindow";


    @Override
    public Void call() throws Exception {
        ExperimentalOptions eopts = PipelineOptionsFactory.create().as(ExperimentalOptions.class);
        List<String> experiments = new ArrayList<>();
        experiments.add("use_deprecated_read");
        eopts.setExperiments(experiments);
        // FlinkPipelineOptions options = PipelineOptionsFactory.create().as(FlinkPipelineOptions.class);
        FlinkPipelineOptions options = eopts.as(FlinkPipelineOptions.class);
        // FlinkPipelineOptions options = PipelineOptionsFactory.create().as(FlinkPipelineOptions.class);
        options.setDisableMetrics(true);
        options.setRunner(FlinkRunner.class);
        options.setJobName(name);
        // options.setShutdownSourcesAfterIdleMs(100L);
        Pipeline p = Pipeline.create(options);

        p
                .apply(Read.from(new CountSource(this.from, this.to)))
                .apply(Window.into(FixedWindows.of(org.joda.time.Duration.millis(this.windowDuration.toMillis()))))
                .apply(Combine.globally(Count.<Long>combineFn()).withoutDefaults())
                .apply(ParDo.of(new Printer<>(name + ": %d\n")));


        p.run().waitUntilFinish();


        return null;
    }
}
