package no.spaeren.thesis.benchmarks.beam;

import no.spaeren.thesis.benchmarks.beam.helpers.Printer;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "BeamSimple", mixinStandardHelpOptions = true, description = "A simple beam job")
public class BeamSimple implements Callable<Void> {

    @CommandLine.Option(names = {"--from"}, defaultValue = "0")
    final Long from = 0L;

    @CommandLine.Option(names = {"--to"}, defaultValue = "1000")
    final Long to = 1000L;


    @Override
    public Void call() {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(FlinkRunner.class);
        Pipeline p = Pipeline.create(options);


        final PCollection<Long> ds = p.apply(GenerateSequence.from(this.from).to(this.to));
        final PCollection<Long> db = ds.apply(MapElements.into(TypeDescriptors.longs()).via((Long x) -> x * 2));

        db.apply(ParDo.of(new Printer<>("BeamSimple: %d\n")));


        p.run().waitUntilFinish();

        return null;
    }
}
