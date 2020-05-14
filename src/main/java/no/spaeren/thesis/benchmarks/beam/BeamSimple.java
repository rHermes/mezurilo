package no.spaeren.thesis.benchmarks.beam;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class BeamSimple {
    public void execute(String... args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(FlinkRunner.class);
        Pipeline p = Pipeline.create(options);


        final PCollection<Long> ds = p.apply(GenerateSequence.from(0));

        final PCollection<Long> db = ds.apply(MapElements.into(TypeDescriptors.longs()).via((Long x) -> x * 2));

        db.apply(ParDo.of(new Printer<>("BeamSimple: %d\n")));

        PipelineResult.State state = p.run().waitUntilFinish();

        System.out.println(state.toString());

    }
}
