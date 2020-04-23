package org.example.kstopologyvisualizations;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.CogroupedKStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

@Slf4j
public class OtherTopologyVisualizationTest {

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

    /**
     * Cogroup
     */

    @Test
    public void showCogroupedKStream() {
        StreamsBuilder coGroupingStream = coGroupingStream();
        log.info(coGroupingStream.build().describe().toString());
    }

    private StreamsBuilder coGroupingStream() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> fooStream =
                streamsBuilder.stream("foo");
        KStream<String, String> barStream =
                streamsBuilder.stream("bar");
        CogroupedKStream<String, String> coGroupedFoos =
                fooStream.groupByKey().cogroup((bytes, s, o) -> o + s);
        CogroupedKStream<String, String> coGroupedFoosAndBars =
                coGroupedFoos.cogroup(
                        barStream.groupByKey(),
                        (bytes, s, o) -> o + s);
        coGroupedFoosAndBars.aggregate(() -> "").toStream()
                .to("foosAndBars");
        return streamsBuilder;
    }

    private void assertCoGroupedOutput(TopologyTestDriver topologyTestDriver) {
        TestOutputTopic<String, String> foosAndBarsCoGroupedAggregated = topologyTestDriver.createOutputTopic("foosAndBars", new StringDeserializer(), new StringDeserializer());
        assertEquals(new KeyValue<>("1", "a"), foosAndBarsCoGroupedAggregated.readKeyValue());
        assertEquals(new KeyValue<>("2", "b"), foosAndBarsCoGroupedAggregated.readKeyValue());
        assertEquals(new KeyValue<>("1", "ac"), foosAndBarsCoGroupedAggregated.readKeyValue());
        assertEquals(new KeyValue<>("2", "bd"), foosAndBarsCoGroupedAggregated.readKeyValue());
    }

    @Test
    public void testCoGroup() {
        Properties properties = new Properties();
        properties.put("application.id", "cogroup-test-app");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(coGroupingStream().build(), properties)) {
            TestInputTopic<String, String> fooInputTopic =
                    topologyTestDriver.createInputTopic(
                            "foo", new StringSerializer(), new StringSerializer());
            TestInputTopic<String, String> barInputTopic =
                    topologyTestDriver.createInputTopic(
                            "bar", new StringSerializer(), new StringSerializer());
            fooInputTopic.pipeInput("1", "a");
            barInputTopic.pipeInput("2", "b");
            barInputTopic.pipeInput("1", "c");
            fooInputTopic.pipeInput("2", "d");
            fooInputTopic.pipeInput("2", "e");
            assertCoGroupedOutput(topologyTestDriver);
        }
    }

}
