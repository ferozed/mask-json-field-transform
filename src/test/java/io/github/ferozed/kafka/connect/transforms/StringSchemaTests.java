package io.github.ferozed.kafka.connect.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringSchemaTests extends BaseTests {

    @Test
    public void testSimpleStringInValue() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField.Value();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.REPLACEMENT_FIELD_PATH, "/foo/bar"
                )
        );

        Schema valueSchema = SchemaBuilder.STRING_SCHEMA;


        ObjectNode node = (ObjectNode)mapper.createObjectNode();

        node.set("foo",
                mapper.createObjectNode().put("bar", "ssn")
        );

        String value = mapper.writeValueAsString(node);

        SinkRecord sinkRecord = new SinkRecord(
                "topic",
                0,
                SchemaBuilder.STRING_SCHEMA,
                "key",
                SchemaBuilder.STRING_SCHEMA,
                value,
                0
        );

        ConnectRecord transformedRecord = maskJsonField.apply(sinkRecord);

        assertValue((String)transformedRecord.value(), "/foo/bar", "");
    }

    @Test
    public void testSimpleStringInKey() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField.Key();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.REPLACEMENT_FIELD_PATH, "/foo/bar"
                )
        );

        Schema valueSchema = SchemaBuilder.STRING_SCHEMA;
        Schema keySchema = SchemaBuilder.STRING_SCHEMA;


        ObjectNode node = (ObjectNode)mapper.createObjectNode();

        node.set("foo",
                mapper.createObjectNode().put("bar", "ssn")
        );

        String value = mapper.writeValueAsString(node);

        SinkRecord sinkRecord = new SinkRecord(
                "topic",
                0,
                SchemaBuilder.STRING_SCHEMA,
                value,
                SchemaBuilder.STRING_SCHEMA,
                value,
                0
        );

        ConnectRecord transformedRecord = maskJsonField.apply(sinkRecord);

        assertValue((String)transformedRecord.value(), "/foo/bar", "ssn");
        assertValue((String)transformedRecord.key(), "/foo/bar", "");
    }

    @Test
    public void testSimpleStrinWithoutReplacement() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField.Value();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.REPLACEMENT_FIELD_PATH, "/foo/bar"
                )
        );

        Schema valueSchema = SchemaBuilder.STRING_SCHEMA;


        ObjectNode node = (ObjectNode)mapper.createObjectNode();

        String value = "{\"foo\":{\"nothing\":\"1\"}}";

        SinkRecord sinkRecord = new SinkRecord(
                "topic",
                0,
                SchemaBuilder.STRING_SCHEMA,
                "key",
                SchemaBuilder.STRING_SCHEMA,
                value,
                0
        );

        ConnectRecord transformedRecord = maskJsonField.apply(sinkRecord);

        Assertions.assertEquals(value, (String)transformedRecord.value());
    }

    @Test
    public void testInvalidJsonWithoutReplacement() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField.Value();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.REPLACEMENT_FIELD_PATH, "/foo/bar"
                )
        );

        Schema valueSchema = SchemaBuilder.STRING_SCHEMA;


        ObjectNode node = (ObjectNode)mapper.createObjectNode();

        String value = "{";

        SinkRecord sinkRecord = new SinkRecord(
                "topic",
                0,
                SchemaBuilder.STRING_SCHEMA,
                "key",
                SchemaBuilder.STRING_SCHEMA,
                value,
                0
        );

        ConnectRecord transformedRecord = maskJsonField.apply(sinkRecord);

        Assertions.assertEquals(value, (String)transformedRecord.value());
    }

    @Test
    public void testEmptyJson() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField.Value();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.REPLACEMENT_FIELD_PATH, "/foo/bar"
                )
        );

        Schema valueSchema = SchemaBuilder.STRING_SCHEMA;


        ObjectNode node = (ObjectNode)mapper.createObjectNode();

        String value = "{}";

        SinkRecord sinkRecord = new SinkRecord(
                "topic",
                0,
                SchemaBuilder.STRING_SCHEMA,
                "key",
                SchemaBuilder.STRING_SCHEMA,
                value,
                0
        );

        ConnectRecord transformedRecord = maskJsonField.apply(sinkRecord);

        Assertions.assertEquals(value, (String)transformedRecord.value());
    }
}
