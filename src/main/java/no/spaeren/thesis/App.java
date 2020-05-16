package no.spaeren.thesis;

import no.spaeren.thesis.benchmarks.beam.BeamSimple;
import no.spaeren.thesis.benchmarks.flink.FlinkSimple;
import no.spaeren.thesis.benchmarks.flink.FlinkWatermark;
import picocli.CommandLine;


@CommandLine.Command(
        name = "mezurilo",
        version = "1.0",
        mixinStandardHelpOptions = true,
        subcommands = {
                FlinkSimple.class, BeamSimple.class, FlinkWatermark.class
        }
)
public class App {

    public static void main(String... args) {
        @SuppressWarnings("InstantiationOfUtilityClass") int exitCode = new CommandLine(new App()).execute(args);
    }
}
