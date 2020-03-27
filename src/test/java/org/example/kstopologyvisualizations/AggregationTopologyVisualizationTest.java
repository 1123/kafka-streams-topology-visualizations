package org.example.kstopologyvisualizations;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.internals.suppress.EagerBufferConfigImpl;
import org.apache.kafka.streams.kstream.internals.suppress.FinalResultsSuppressionBuilder;
import org.apache.kafka.streams.kstream.internals.suppress.StrictBufferConfigImpl;
import org.junit.Test;

import java.time.Duration;

import static java.time.Duration.ofMinutes;

@Slf4j
public class AggregationTopologyVisualizationTest {

    /**
     * Grouping, Windowing and Aggregations
     */

    @Test
    public void showGroupByKeyAndCountTopology() {
        StreamsBuilder streamsBuilder =
                new StreamsBuilder();
        KStream<String, Foo> fooStream = streamsBuilder
                .stream("foo");
        fooStream
                .groupByKey()
                .count()
                .toStream()
                .to("counted-by-key");
        log.info(
                streamsBuilder.build().describe().toString()
        );
    }


    @Test
    public void showGroupByKeyAndReduceTopology() {
        StreamsBuilder streamsBuilder =
                new StreamsBuilder();
        KStream<String, String> fooKStream =
                streamsBuilder.stream("foo");
        fooKStream.groupByKey()
                .reduce((value1, value2) -> value1 + value2)
                .toStream()
                .to("reduced");
        log.info(streamsBuilder.build().describe().toString());
    }

    @Test
    public void showRepartitionTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Foo> fooKStream =
                streamsBuilder.stream("foo");
        fooKStream
                .groupBy((k,v) -> v.field1)
                .count()
                .toStream()
                .to("count-by-field1");
        log.info(streamsBuilder.build().describe().toString());
    }

    @Test
    public void showWindowByTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Foo> fooKStream =
                streamsBuilder.stream("foo");
        fooKStream.groupByKey()
                .windowedBy(JoinWindows.of(
                        ofMinutes(1)
                ))
                .count()
                .toStream()
                .to("windowed");
        log.info(streamsBuilder.build().describe().toString());
    }

    @Test
    public void showSuppressTopology() {
        StreamsBuilder streamsBuilder =
                new StreamsBuilder();
        KStream<String, Foo> fooStream = streamsBuilder
                .stream("foo");
        fooStream
                .groupByKey()
                .windowedBy(JoinWindows.of(ofMinutes(1)))
                .count()
                .suppress(
                    Suppressed.untilWindowCloses(
                        Suppressed.BufferConfig.unbounded()
                    )
                )
                .toStream()
                .to("counted-by-key");
        log.info(
                streamsBuilder.build().describe().toString()
        );
    }

}
