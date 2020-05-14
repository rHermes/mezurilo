package no.spaeren.thesis.benchmarks.beam;

import org.apache.beam.sdk.transforms.DoFn;

public class Printer<T> extends DoFn<T, Void> {
    final String fmtstr;

    public Printer(String fmtstr) {
        this.fmtstr = fmtstr;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        System.out.printf(this.fmtstr, c.element());
    }
}
