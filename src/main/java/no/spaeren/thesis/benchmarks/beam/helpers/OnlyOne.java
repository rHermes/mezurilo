package no.spaeren.thesis.benchmarks.beam.helpers;

import org.apache.beam.sdk.transforms.DoFn;

public class OnlyOne extends DoFn<Long,Long> {
    final Long match;

    public OnlyOne(Long match) {
        this.match = match;
    }

    @ProcessElement
    public void processElement(@Element Long in, OutputReceiver<Long> out) {
        if (in.equals(this.match)) {
            out.output(in);
        }
    }
}
