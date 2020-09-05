package no.spaeren.thesis.benchmarks.flink;

import no.spaeren.thesis.benchmarks.flink.wikimedia.Event;
import no.spaeren.thesis.benchmarks.flink.wikimedia.EventEntity;
import no.spaeren.thesis.benchmarks.flink.wikimedia.EventType;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import picocli.CommandLine;

import java.time.Duration;
import java.util.concurrent.Callable;
@CommandLine.Command(name = "FlinkWikiHistoryTopEditors", mixinStandardHelpOptions = true,
        description = "A benchmark for the number of editors")
public class FlinkWikipediaHistoryTopEditors implements Callable<Void> {
    @Override
    public Void call() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.setBufferTimeout(10);
        // env.getConfig().setAutoWatermarkInterval(1000);



        // final String fileNameInput = "file:///home/rhermes/madsci/thesis/data/mediawiki_history/jjjj.tsv";
        final String fileNameInput = "file:///home/rhermes/madsci/thesis/data/mediawiki_history/2020-07.enwiki.2016-04.sorted.tsv";
        /// final DataStreamSource<String> linesIn = env.readTextFile("file:///home/rhermes/madsci/thesis/data/mediawiki_history/2020-07.enwiki.2016-04.tsv");
        // final DataStreamSource<String> linesIn = env.readTextFile(fileNameInput);

        final DataStream<String> linesIn = env.readFile

        final SingleOutputStreamOperator<Event> jj = linesIn.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                return new Event(value);
            }
        });

        final DataStream<Event> props = jj.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Event>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(Event element) {
                return element.eventTimestamp.toEpochSecond() * 1000;
            }
        });

        final KeyedStream<Event, EventEntity> praps = props.keyBy((e) -> e.eventEntity);
        // praps.countWindow(1000).sum("something").print("WHRF");

        praps.window(TumblingEventTimeWindows.of(Time.hours(1))).sum("something").print("JAJ!");

        // praps.window(TumblingEventTimeWindows.of(Time.hours(1))).sum("something").print("WHATA");

//        props.keyBy(e -> e.eventEntity).timeWindow(Time.hours(1)).sum("something").print("WHATA");


        env.execute("FlinkWikipediaHistoryTopEditors");
        return null;
    }
}
