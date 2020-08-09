package no.spaeren.thesis.benchmarks.beam.helpers;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CountSource extends UnboundedSource<Long, UnboundedSource.CheckpointMark> {

    private final Long from;
    private final Long to;

    public CountSource() {
        this.from = 0L;
        this.to = Long.MAX_VALUE;
    }

    public CountSource(Long from, Long to) {
        this.from = from;
        this.to = to;
    }

    /**
     * Returns a list of {@code UnboundedSource} objects representing the instances of this source
     * that should be used when executing the workflow. Each split should return a separate partition
     * of the input data.
     *
     * <p>For example, for a source reading from a growing directory of files, each split could
     * correspond to a prefix of file names.
     *
     * <p>Some sources are not splittable, such as reading from a single TCP stream. In that case,
     * only a single split should be returned.
     *
     * <p>Some data sources automatically partition their data among readers. For these types of
     * inputs, {@code n} identical replicas of the top-level source can be returned.
     *
     * <p>The size of the returned list should be as close to {@code desiredNumSplits} as possible,
     * but does not have to match exactly. A low number of splits will limit the amount of parallelism
     * in the source.
     *
     * @param desiredNumSplits
     * @param options
     */
    @Override
    public List<? extends UnboundedSource<Long, CheckpointMark>> split(int desiredNumSplits, PipelineOptions options) throws Exception {

        if (desiredNumSplits < 2) {
            return Collections.singletonList(this);
        }
        // We need to figure out when this is called
        List sources = new ArrayList();

        Long nums = this.to - this.from;
        Long e = nums / desiredNumSplits;
        Long o = nums % desiredNumSplits;

        Long current = this.from;
        for (int i = 0; i < desiredNumSplits; i++) {
            Long span = e;
            if (o > 0) {
                span += 1;
                o -= 1;
            }

            sources.add(new CountSource(current, current+span));
            current += span;
        }

        return sources;
    }

    /**
     * Create a new {@link UnboundedReader} to read from this source, resuming from the given
     * checkpoint if present.
     *
     * @param options
     * @param checkpointMark
     */
    @Override
    public UnboundedReader<Long> createReader(PipelineOptions options, @Nullable CheckpointMark checkpointMark) throws IOException {
        return new CountSourceReader(this, this.from, this.to);
    }

    /**
     * Returns a {@link Coder} for encoding and decoding the checkpoints for this source.
     */
    @Override
    public Coder<CheckpointMark> getCheckpointMarkCoder() {
        return null;
    }

    /**
     * Returns the {@code Coder} to use for the data read from this source.
     */
    @Override
    public Coder<Long> getOutputCoder() {
        return VarLongCoder.of();
    }
}
