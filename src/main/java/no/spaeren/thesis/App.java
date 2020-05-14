package no.spaeren.thesis;

import no.spaeren.thesis.benchmarks.flink.FlinkSimple;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

enum Benchmarks {
    FlinkSimple,
}

@CommandLine.Command(name = "mezurilo", mixinStandardHelpOptions = true, version = "1.0")
public class App implements Callable<Integer> {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec commandSpec;

    @Option(names = "--benchmark", required = true, description = "Benchmark to run: ${COMPLETION-CANDIDATES}")
    Benchmarks benchmark;

    public static void main(String... args) {
        int exitCode = new CommandLine(new App()).execute(args);
    }


    @Override
    public Integer call() throws Exception {
        System.out.println("Here we are!");


        switch (this.benchmark) {
            case FlinkSimple:
                new FlinkSimple().execute();
                break;
        }

        return 0;
    }
}
