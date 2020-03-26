package org.example.kstopologyvisualizations;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

@Slf4j
public class TopologyVisualizationTest {

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
                        Duration.ofMinutes(1)
                ))
                .count()
                .toStream()
                .to("windowed");
        log.info(streamsBuilder.build().describe().toString());
    }

    /**
     * Joins: stream-stream, stream-table, table-table
     */

    @Test
    public void showKStreamKStreamJoinTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> fooStream =
                streamsBuilder.stream("foo");
        KStream<String, String> barStream =
                streamsBuilder.stream("bar");
        fooStream.join(
                barStream,
                (v1, v2) -> v1 + v2,
                JoinWindows.of(Duration.ofMinutes(1))
        );
        log.info(streamsBuilder.build().describe().toString());
    }

    @Test
    public void showKStreamKTableJoinTopology() {
        StreamsBuilder streamsBuilder =
                new StreamsBuilder();
        KStream<String, String> fooStream =
                streamsBuilder.stream("foo");
        KTable<String, String> barTable =
                streamsBuilder.table("bar");
        fooStream
                .join(barTable, (v1, v2) -> v1 + v2)
                .to("joined");
        log.info(
                streamsBuilder.build().describe().toString()
        );
    }

    @Test
    public void showTableTableJoin() {
        StreamsBuilder streamsBuilder =
                new StreamsBuilder();
        KTable<String, Foo> fooTable =
                streamsBuilder.table("foo");
        KTable<String, Bar> barTable =
                streamsBuilder.table("bar");
        fooTable.join(barTable, Foo::join)
                .toStream().to("joined-tables");
        log.info(streamsBuilder.build().describe().toString());
    }

    @Test
    public void showTableTableFKJoin() {
        StreamsBuilder streamsBuilder =
                new StreamsBuilder();
        KTable<String, Foo> fooTable =
                streamsBuilder.table("foo");
        KTable<String, Bar> barTable =
                streamsBuilder.table("bar");
        fooTable.join(
                barTable,
                Foo::getBarId,
                Foo::join
        ).toStream().to("joined-tables");
        log.info(streamsBuilder.build().describe().toString());
    }


    /**
     * Branch
     */

    @Test
    public void showBranchedStreams() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Foo> fooKStream =
                streamsBuilder.stream("foo");
        KStream<String, Foo>[] branchedStreams =
                fooKStream.branch(
                        (k, v) -> v.field1.equals("1"),
                        (k, v) -> v.field1.equals("2")
                );
        branchedStreams[0].to("branch1");
        branchedStreams[1].to("branch2");
        log.info(streamsBuilder.build().describe().toString());
    }

}

@Data
class Foo {

    String field1;
    String barId;

    String join(Bar bar) {
        return "joined";
    }

}

@Data
class Bar {
    String field1;
}
