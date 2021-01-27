package no.spaeren.thesis;

import no.spaeren.thesis.benchmarks.beam.BeamNumOperators;
import no.spaeren.thesis.benchmarks.beam.BeamNumbers;
import no.spaeren.thesis.benchmarks.beam.BeamSimple;
import no.spaeren.thesis.benchmarks.beam.BeamSimpleWindow;
import no.spaeren.thesis.benchmarks.flink.*;
import picocli.CommandLine;


@CommandLine.Command(
        name = "mezurilo",
        version = "1.0",
        mixinStandardHelpOptions = true,
        subcommands = {
                FlinkSimple.class, FlinkWatermark.class, FlinkNumOperators.class,
                FlinkSimpleWindow.class, BeamSimpleWindow.class, BeamSimple.class,
                BeamNumOperators.class, FlinkWikipediaHistoryTopEditors.class, BeamNumbers.class,
        }
)
public class App {

    public static void main(String... args) {
        @SuppressWarnings("InstantiationOfUtilityClass") int exitCode = new CommandLine(new App()).execute(args);
    }
}
