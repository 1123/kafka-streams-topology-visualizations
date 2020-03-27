package org.example.kstopologyvisualizations;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

@Data
class Bar {
    String field1;
}
