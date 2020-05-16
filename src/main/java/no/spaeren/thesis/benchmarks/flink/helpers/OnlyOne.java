package no.spaeren.thesis.benchmarks.flink.helpers;

import org.apache.flink.api.common.functions.FilterFunction;

public class OnlyOne<T> implements FilterFunction<T> {

    final T target;

    public OnlyOne(T target) {
        this.target = target;
    }

    @Override
    public boolean filter(T value) {
        return target.equals(value);
    }
}
