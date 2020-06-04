package no.spaeren.thesis.benchmarks.beam.helpers;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class CountSource extends UnboundedSource<Long, UnboundedSource.CheckpointMark> {
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
        return Collections.singletonList(this);
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
        return new CountSourceReader(this);
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
