package no.spaeren.thesis.benchmarks.flink.sources;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;

import java.util.ArrayDeque;
import java.util.Deque;

public class SeqGenSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private final long start;
    private final long end;

    private volatile boolean isRunning = true;

    private transient long mcollected;
    private transient long mcongruence;
    private transient long mstepSize;
    private transient long mtocollect;


    // (collected, congruence, stepsize, toCollect)
    private transient Deque<Tuple4<Long, Long, Long, Long>> valuesToEmit;
    private transient ListState<Tuple4<Long, Long, Long, Long>> checkpointedState;

    /**
     * Creates a source that emits all numbers from the given interval exactly once.
     *
     * @param start Start of the range of numbers to emit.
     * @param end   End of the range of numbers to emit.
     */
    public SeqGenSource(long start, long end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        Preconditions.checkState(this.checkpointedState == null,
                "The " + getClass().getSimpleName() + " has already been initialized.");

        this.checkpointedState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>(
                        "seq-gen-source-state",
                        TypeInformation.of(new TypeHint<Tuple4<Long, Long, Long, Long>>() {
                        })
                )
        );

        this.valuesToEmit = new ArrayDeque<>();
        if (context.isRestored()) {
            // upon restoring
            for (Tuple4<Long, Long, Long, Long> v : this.checkpointedState.get()) {
                this.valuesToEmit.add(v);
            }
        } else {
            // the first time the job is executed
            final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
            final int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            final long congruence = start + taskIdx;

            long totalNoOfElements = Math.abs(end - start + 1);
            final int baseSize = safeDivide(totalNoOfElements, stepSize);
            final int toCollect = (totalNoOfElements % stepSize > taskIdx) ? baseSize + 1 : baseSize;

            this.valuesToEmit.add(Tuple4.of(0L, congruence, (long) stepSize, (long) toCollect));
        }
    }

    @Override
    public void run(SourceContext<Long> ctx) throws NullPointerException {
        while (isRunning && !this.valuesToEmit.isEmpty()) {
            synchronized (ctx.getCheckpointLock()) {
                final Tuple4<Long, Long, Long, Long> outs = this.valuesToEmit.poll();
                if (outs == null) {
                    throw new NullPointerException();
                }

                this.mcollected = outs.f0;
                this.mcongruence = outs.f1;
                this.mstepSize = outs.f2;
                this.mtocollect = outs.f3;
            }

            while (isRunning && this.mcollected < this.mtocollect) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(this.mcollected * this.mstepSize + this.mcongruence);
                    this.mcollected++;
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Preconditions.checkState(this.checkpointedState != null,
                "The " + getClass().getSimpleName() + " state has not been properly initialized.");

        this.checkpointedState.clear();
        this.checkpointedState.add(Tuple4.of(this.mcollected, this.mcongruence, this.mstepSize, this.mtocollect));
        for (Tuple4<Long, Long, Long, Long> v : this.valuesToEmit) {
            this.checkpointedState.add(v);
        }
    }

    private static int safeDivide(long left, long right) {
        Preconditions.checkArgument(right > 0);
        Preconditions.checkArgument(left >= 0);
        Preconditions.checkArgument(left <= Integer.MAX_VALUE * right);
        return (int) (left / right);
    }
}