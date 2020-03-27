package org.example.kstopologyvisualizations;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;

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

}
