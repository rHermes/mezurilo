package no.spaeren.thesis.benchmarks.beam.helpers;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.NoSuchElementException;

public class CountSourceReader extends UnboundedSource.UnboundedReader<Long> {

    private CountSource countSource;

    private Long current;
    private Instant currentTimestamp;

    public CountSourceReader(CountSource countSource) {
        this.countSource = countSource;
    }

    /**
     * Initializes the reader and advances the reader to the first record. If the reader has been
     * restored from a checkpoint then it should advance to the next unread record at the point the
     * checkpoint was taken.
     *
     * <p>This method will be called exactly once. The invocation will occur prior to calling {@link
     * #advance} or {@link #getCurrent}. This method may perform expensive operations that are
     * needed to initialize the reader.
     *
     * <p>Returns {@code true} if a record was read, {@code false} if there is no more input
     * currently available. Future calls to {@link #advance} may return {@code true} once more data
     * is available. Regardless of the return value of {@code start}, {@code start} will not be
     * called again on the same {@code UnboundedReader} object; it will only be called again when a
     * new reader object is constructed for the same source, e.g. on recovery.
     */
    @Override
    public boolean start() throws IOException {
        this.current = 0L;
        return this.advance();
    }

    /**
     * Advances the reader to the next valid record.
     *
     * <p>Returns {@code true} if a record was read, {@code false} if there is no more input
     * available. Future calls to {@link #advance} may return {@code true} once more data is
     * available.
     */
    @Override
    public boolean advance() throws IOException {
        this.current = this.current + 1;
        this.currentTimestamp = Instant.now();
        return true;
    }

    /**
     * Returns the value of the data item that was read by the last {@link #start} or {@link
     * #advance} call. The returned value must be effectively immutable and remain valid
     * indefinitely.
     *
     * <p>Multiple calls to this method without an intervening call to {@link #advance} should
     * return the same result.
     *
     * @throws NoSuchElementException if {@link #start} was never called, or if the last
     *                                {@link #start} or {@link #advance} returned {@code false}.
     */
    @Override
    public Long getCurrent() throws NoSuchElementException {
        if (this.current == null) {
            throw new NoSuchElementException();
        }
        return this.current;
    }

    /**
     * Returns the timestamp associated with the current data item.
     *
     * <p>If the source does not support timestamps, this should return {@code
     * BoundedWindow.TIMESTAMP_MIN_VALUE}.
     *
     * <p>Multiple calls to this method without an intervening call to {@link #advance} should
     * return the same result.
     *
     * @throws NoSuchElementException if the reader is at the beginning of the input and {@link
     *                                #start} or {@link #advance} wasn't called, or if the last {@link #start} or {@link
     *                                #advance} returned {@code false}.
     */
    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (this.currentTimestamp == null) {
            throw new NoSuchElementException();
        }
        return this.currentTimestamp;
    }

    /**
     * Closes the reader. The reader cannot be used after this method is called.
     */
    @Override
    public void close() throws IOException {

    }

    /**
     * Returns a timestamp before or at the timestamps of all future elements read by this reader.
     *
     * <p>This can be approximate. If records are read that violate this guarantee, they will be
     * considered late, which will affect how they will be processed. See {@link
     * Window} for more information on late data and how to
     * handle it.
     *
     * <p>However, this value should be as late as possible. Downstream windows may not be able to
     * close until this watermark passes their end.
     *
     * <p>For example, a source may know that the records it reads will be in timestamp order. In
     * this case, the watermark can be the timestamp of the last record read. For a source that does
     * not have natural timestamps, timestamps can be set to the time of reading, in which case the
     * watermark is the current clock time.
     *
     * <p>See {@link Window} and {@link
     * Trigger} for more information on timestamps and
     * watermarks.
     *
     * <p>May be called after {@link #advance} or {@link #start} has returned false, but not before
     * {@link #start} has been called.
     */
    @Override
    public Instant getWatermark() {
        return this.currentTimestamp.minus(Duration.standardSeconds(1));
    }

    /**
     * Returns a {@link UnboundedSource.CheckpointMark} representing the progress of this {@code UnboundedReader}.
     *
     * <p>If this {@code UnboundedReader} does not support checkpoints, it may return a
     * CheckpointMark which does nothing, like:
     *
     * <pre>{@code
     * public UnboundedSource.CheckpointMark getCheckpointMark() {
     *   return new UnboundedSource.CheckpointMark() {
     *     public void finalizeCheckpoint() throws IOException {
     *       // nothing to do
     *     }
     *   };
     * }
     * }</pre>
     *
     * <p>All elements read between the last time this method was called (or since this reader was
     * created, if this method has not been called on this reader) until this method is called will
     * be processed together as a bundle. (An element is considered 'read' if it could be returned
     * by a call to {@link #getCurrent}.)
     *
     * <p>Once the result of processing those elements and the returned checkpoint have been durably
     * committed, {@link UnboundedSource.CheckpointMark#finalizeCheckpoint} will be called at most once at some
     * later point on the returned {@link UnboundedSource.CheckpointMark} object. Checkpoint finalization is
     * best-effort, and checkpoints may not be finalized. If duplicate elements may be produced if
     * checkpoints are not finalized in a timely manner, {@link UnboundedSource#requiresDeduping()}
     * should be overridden to return true, and {@link UnboundedSource.UnboundedReader#getCurrentRecordId()} should
     * be overridden to return unique record IDs.
     *
     * <p>A checkpoint will be committed to durable storage only if all all previous checkpoints
     * produced by the same reader have also been committed.
     *
     * <p>The returned object should not be modified.
     *
     * <p>May not be called before {@link #start} has been called.
     */
    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return null;
    }

    /**
     * Returns the {@link UnboundedSource} that created this reader. This will not change over the
     * life of the reader.
     */
    @Override
    public UnboundedSource<Long, ?> getCurrentSource() {
        return this.countSource;
    }
}
