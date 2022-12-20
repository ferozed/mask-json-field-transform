package com.github.ferozed.json.mask;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MaskJsonFieldTest {

    ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testSimpleString() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.CONNECT_FIELD_NAME, "user_data",
                        MaskJsonFieldConfig.OUTER_FIELD_PATH, "/foo",
                        MaskJsonFieldConfig.MASK_FIELD_NAME, "bar"
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

        assertStringValue((String)transformedRecord.value(), "/foo/bar", "");
    }

    @Test
    public void testToplevelFieldInConnectStruct() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.CONNECT_FIELD_NAME, "document",
                        MaskJsonFieldConfig.OUTER_FIELD_PATH, "",
                        MaskJsonFieldConfig.MASK_FIELD_NAME, "ssn"
                )
        );

        Schema documentSchema = SchemaBuilder.struct()
                .field("document", Schema.STRING_SCHEMA)
                .schema();

        Schema valueSchema = SchemaBuilder.struct()
                .field("database", Schema.STRING_SCHEMA)
                .field("document", Schema.STRING_SCHEMA
                ).schema();

        Struct value = new Struct(valueSchema)
                .put("database", "dynamo")
                .put("document", "{\"first_name\": \"john\", \"last_name\": \"doe\", \"ssn\": \"122-12-1212\"}");

        SinkRecord sinkRecord = new SinkRecord(
                "topic",
                0,
                SchemaBuilder.STRING_SCHEMA,
                "key",
                valueSchema,
                value,
                0
        );

        ConnectRecord transformedRecord = maskJsonField.apply(sinkRecord);

        Struct data = (Struct)transformedRecord.value();
        String documentJson = data.getString("document");
        assertStringValue(documentJson, "/ssn", "");
    }

    @Test
    public void testNestedFieldInConnectStruct() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.CONNECT_FIELD_NAME, "inner.document",
                        MaskJsonFieldConfig.OUTER_FIELD_PATH, "",
                        MaskJsonFieldConfig.MASK_FIELD_NAME, "ssn"
                )
        );

        Schema innerSchema = SchemaBuilder.struct()
                .field("document", Schema.STRING_SCHEMA)
                .schema();

        Schema valueSchema = SchemaBuilder.struct()
                .field("inner", innerSchema)
                .field("database", Schema.STRING_SCHEMA
                ).schema();

        Struct value = new Struct(valueSchema)
                .put("database", "dynamo")
                .put("inner",
                        new Struct(innerSchema)
                                .put("document", "{\"first_name\": \"john\", \"last_name\": \"doe\", \"ssn\": \"122-12-1212\"}")
                );

        SinkRecord sinkRecord = new SinkRecord(
                "topic",
                0,
                SchemaBuilder.STRING_SCHEMA,
                "key",
                valueSchema,
                value,
                0
        );

        ConnectRecord transformedRecord = maskJsonField.apply(sinkRecord);

        Struct data = (Struct)transformedRecord.value();
        String documentJson = data.getStruct("inner").getString("document");
        assertStringValue(documentJson, "/ssn", "");
    }

    void assertStringValue(String jsonPayload, String path, String value) throws JsonProcessingException {
        JsonNode root = mapper.readTree(jsonPayload);
        JsonPointer pointer = JsonPointer.compile(path);
        JsonNode targetNode = root.at(pointer);

        Assertions.assertTrue(targetNode.isTextual() && targetNode.textValue().contentEquals(""), String.format("%s in %s = \"%s\"", path, jsonPayload, value));
    }

    @Test
    void jsonPointerTest() throws Exception {
        String payload = "{\"foo\": { \"bar\": \"ssn\" }}";

        print(
                "{\"foo\": { \"bar\": \"ssn\" }}",
                "/foo/bar"
        );

        print(
                "{\"foo\": { \"bar\": \"ssn\" }, \"ssn\": \"111-22-1212\"}",
                "/ssn"
        );


        /*
        if (parentNode instanceof  ObjectNode) {
            ObjectNode o = (ObjectNode) parentNode;
            o.put(targetPointer.getMatchingProperty(), "");
        }

        System.out.printf("Input: [%s]\n", payload);
        System.out.printf("Otput: [%s]\n", mapper.writeValueAsBytes(root));
         */

    }

    void print(String payload, String path) throws Exception{
        System.out.printf("[%s] %s\n", payload, path);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(payload);
        JsonPointer targetPointer = JsonPointer.compile(path);

        System.out.printf("        targetPointer mp=%s head.mp=%s tail.mp=%s\n",
                targetPointer.getMatchingProperty(),
                targetPointer.head() != null ? targetPointer.head().getMatchingProperty(): "null",
                targetPointer.tail() != null ? targetPointer.tail().getMatchingProperty(): "null"
                );

        JsonPointer parentOfTargetPointer = targetPointer.head();

        System.out.printf("parentOfTargetPointer mp=%s head.mp=%s tail.mp=%s\n",
                parentOfTargetPointer.getMatchingProperty(),
                parentOfTargetPointer.head() != null ? parentOfTargetPointer.head().getMatchingProperty(): "null",
                parentOfTargetPointer.tail() != null ? parentOfTargetPointer.tail().getMatchingProperty(): "null"
        );


        JsonNode parentNode = root.at(parentOfTargetPointer);
        JsonNode targetNode = root.at(targetPointer);

        System.out.printf("parentNode: %s\n", mapper.writeValueAsString(parentNode));
        System.out.printf("targetNode: %s\n", mapper.writeValueAsString(targetNode));

    }
}
