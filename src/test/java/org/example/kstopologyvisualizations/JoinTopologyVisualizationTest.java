package org.example.kstopologyvisualizations;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.Test;

import java.time.Duration;

@Slf4j
public class JoinTopologyVisualizationTest {

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


}

