package no.spaeren.thesis.benchmarks.flink.helpers;

import org.apache.flink.api.common.functions.MapFunction;

public class MapIdentity<T> implements MapFunction<T, T> {

    @Override
    public T map(T value) {
        return value;
    }
}
