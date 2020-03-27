package org.example.kstopologyvisualizations;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

@Slf4j
public class BasicTopologyVisualizationTest {

    /**
     * Sources and Sinks
     **/

    @Test
    public void showSourcesAndSinks() {
        StreamsBuilder streamsBuilder =
                new StreamsBuilder();
        streamsBuilder.stream(Arrays.asList("source1", "source2")).to("sink");
        log.info(
                streamsBuilder.build().describe().toString()
        );
    }

    /**
     * Basic operations: map, filter, selectKey, etc
     */

    @Test
    public void showMapValuesTopology() {
        StreamsBuilder streamsBuilder =
                new StreamsBuilder();
        KStream<String, Foo> fooStream = streamsBuilder
                .stream("foo");
        fooStream
                .mapValues(v -> v.field1)
                .to("mapped");
        log.info(
                streamsBuilder.build().describe().toString()
        );
    }

    @Test
    public void showMapTopology() {
        StreamsBuilder streamsBuilder =
                new StreamsBuilder();
        KStream<String, Foo> fooStream = streamsBuilder
                .stream("foo");
        fooStream
                .map((k,v) -> new KeyValue<>(k + k, v.field1))
                .to("mapped");
        log.info(
                streamsBuilder.build().describe().toString()
        );
    }


    @Test
    public void showSelectKeyTopology() {
        StreamsBuilder streamsBuilder =
                new StreamsBuilder();
        KStream<String, Foo> fooStream = streamsBuilder
                .stream("foo");
        fooStream
                .selectKey((k,v) -> v.field1)
                .to("rekeyed");
        log.info(
                streamsBuilder.build().describe().toString()
        );
    }

}

