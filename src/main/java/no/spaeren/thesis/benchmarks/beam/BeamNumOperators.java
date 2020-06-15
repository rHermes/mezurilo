package no.spaeren.thesis.benchmarks.beam;

import no.spaeren.thesis.benchmarks.beam.helpers.CountSource;
import no.spaeren.thesis.benchmarks.beam.helpers.Printer;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "BeamNumOperators", mixinStandardHelpOptions = true,
        description = "A number of operators benchmark")
public class BeamNumOperators implements Callable<Void> {

    @CommandLine.Option(names = {"--from"}, defaultValue = "0")
    final Long from = 0L;

    @CommandLine.Option(names = {"--to"}, defaultValue = "100000000")
    final Long to = 100000000L;


    @CommandLine.Option(names = {"--nodes"}, defaultValue = "1", description = "Number of operator nodes")
    final Long numberOfOperators = 1L;


    @Override
    public Void call() throws Exception {
        FlinkPipelineOptions options = PipelineOptionsFactory.create().as(FlinkPipelineOptions.class);
        options.setDisableMetrics(true);
        options.setRunner(FlinkRunner.class);
        options.setJobName("BeamNumOperators");
        // options.setShutdownSourcesAfterIdleMs(100L);
        Pipeline p = Pipeline.create(options);

        PCollection<Long> stopup = p
                .apply(Read.from(new CountSource(this.from, this.to)));

        for (long i = 0; i < this.numberOfOperators; i++) {
            stopup = stopup.apply(MapElements.into(TypeDescriptors.longs()).via((Long l) -> l));
        }

        stopup.apply(Filter.equal(this.to - 1)).apply(ParDo.of(new Printer<>("BeamNumOperators: %d\n")));


        p.run().waitUntilFinish();

        return null;
    }
}
