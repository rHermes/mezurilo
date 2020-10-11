package no.spaeren.thesis.benchmarks.beam;

import no.spaeren.thesis.benchmarks.beam.helpers.CountSource;
import no.spaeren.thesis.benchmarks.beam.helpers.MapIdentity;
import no.spaeren.thesis.benchmarks.beam.helpers.OnlyOne;
import no.spaeren.thesis.benchmarks.beam.helpers.Printer;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
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

    private enum PosEncoders {
        None,
        Beam,
        Fst,
        Serializable,
    }

    @CommandLine.Option(names = {"--encoder"}, description = "Valid values: ${COMPLETION-CANDIDATES}")
    final PosEncoders posEncoder = PosEncoders.None;

    @CommandLine.Option(names = {"--faster-copy"}, description = "Use the faster copy patch")
    final Boolean useFasterCopy = false;

    @Override
    public Void call() throws Exception {
        FlinkPipelineOptions options = PipelineOptionsFactory.create().as(FlinkPipelineOptions.class);
        options.setDisableMetrics(true);
        options.setRunner(FlinkRunner.class);
        options.setFasterCopy(useFasterCopy);
        options.setJobName("BeamNumOperators");

        System.out.println(useFasterCopy);
        // options.setShutdownSourcesAfterIdleMs(100L);
        Pipeline p = Pipeline.create(options);

        switch (this.posEncoder) {
            case None:
                break;
            case Serializable:
                p.getCoderRegistry().registerCoderForType(TypeDescriptor.of(Long.class), SerializableCoder.of(Long.class));
                break;
            case Beam:
                p.getCoderRegistry().registerCoderForType(TypeDescriptor.of(Long.class), VarLongCoder.of());
                break;
        }

        PCollection<Long> stopup = p
                .apply(Read.from(new CountSource(this.from, this.to)));



        for (long i = 0; i < this.numberOfOperators; i++) {
            stopup = stopup.apply(ParDo.of(new MapIdentity()));
        }


        stopup.apply(ParDo.of(new OnlyOne(this.to - 1))).apply(ParDo.of(new Printer<>("BeamNumOperators: %d\n")));


        p.run().waitUntilFinish();

        return null;
    }
}
