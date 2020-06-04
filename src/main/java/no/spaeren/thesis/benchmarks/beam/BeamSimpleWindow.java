package no.spaeren.thesis.benchmarks.beam;

import no.spaeren.thesis.benchmarks.beam.helpers.CountSource;
import no.spaeren.thesis.benchmarks.beam.helpers.Printer;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import picocli.CommandLine;

import java.time.Duration;
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


    @Override
    public Void call() throws Exception {
        FlinkPipelineOptions options = PipelineOptionsFactory.create().as(FlinkPipelineOptions.class);
        options.setDisableMetrics(true);
        options.setRunner(FlinkRunner.class);
        Pipeline p = Pipeline.create(options);



        // GenerateSequence gs = GenerateSequence.from(0).to(100000000).withRate(100, org.joda.time.Duration.standardSeconds(1L));
//
//        p
//                .apply(gs)
//                .apply(Window.into(FixedWindows.of(org.joda.time.Duration.standardSeconds(5L))))
//                .apply(Combine.globally(Count.<Long>combineFn()).withoutDefaults())
//                .apply(MapElements.into(TypeDescriptors.strings()).via((Long x) -> x.toString()))
//                .apply(TextIO.write().to("thiswont-work)"));

        p
                .apply(Read.from(new CountSource()))
                .apply(Window.into(FixedWindows.of(org.joda.time.Duration.standardSeconds(5L))))
                .apply(Combine.globally(Count.<Long>combineFn()).withoutDefaults())
                .apply(ParDo.of(new Printer<>("BeamSimpleWindow: %d\n")));

        System.out.println("BEFORE THE CRAZY!");
        PipelineResult hgj = p.run();// .waitUntilFinish();
        System.out.println("WE ARE HERE");
        PipelineResult.State rstate = hgj.waitUntilFinish(org.joda.time.Duration.standardMinutes(1));
        System.out.println("This is funny");
        if (rstate == null) {
            hgj.cancel();
        }
        System.out.println("HERE WE ARE AGAIN");


        return null;
    }
}
