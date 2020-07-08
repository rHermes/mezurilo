package no.spaeren.thesis.benchmarks.beam.helpers;

import org.apache.beam.sdk.transforms.DoFn;

public class MapIdentity extends DoFn<Long, Long> {
    @ProcessElement
    public void processElement(@Element Long in, OutputReceiver<Long> out) {
        out.output(in);
    }
}
