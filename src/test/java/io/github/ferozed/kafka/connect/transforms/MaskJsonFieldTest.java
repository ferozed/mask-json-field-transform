/**
 * Copyright © 2022 Feroze Daud (ferozed DOT oss AT gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.ferozed.kafka.connect.transforms;

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
    public void testSimpleStringInValue() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField.Value();
        maskJsonField.configure(
                ImmutableMap.of(
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
    public void testSimpleStringInKey() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField.Key();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.OUTER_FIELD_PATH, "/foo",
                        MaskJsonFieldConfig.MASK_FIELD_NAME, "bar"
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

        assertStringValue((String)transformedRecord.value(), "/foo/bar", "ssn");
        assertStringValue((String)transformedRecord.key(), "/foo/bar", "");
    }

    @Test
    public void testSimpleStrinWithoutReplacement() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField.Value();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.OUTER_FIELD_PATH, "/foo",
                        MaskJsonFieldConfig.MASK_FIELD_NAME, "bar"
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
                        MaskJsonFieldConfig.OUTER_FIELD_PATH, "/foo",
                        MaskJsonFieldConfig.MASK_FIELD_NAME, "bar"
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
                        MaskJsonFieldConfig.OUTER_FIELD_PATH, "/foo",
                        MaskJsonFieldConfig.MASK_FIELD_NAME, "bar"
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

    @Test
    public void testToplevelFieldInConnectKeyStruct() throws JsonProcessingException {
        testToplevelFieldInConnectStructLib(true);
    }

    @Test
    public void testToplevelFieldInConnectValueStruct() throws JsonProcessingException {
        testToplevelFieldInConnectStructLib(false);
    }

    @Test

    private void testToplevelFieldInConnectStructLib(boolean isKey) throws JsonProcessingException {
        MaskJsonField maskJsonField = isKey ? new MaskJsonField.Key() : new MaskJsonField.Value();
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

        SinkRecord sinkRecord = null;

        if (isKey) {
            sinkRecord = new SinkRecord(
                    "topic",
                    0,
                    valueSchema,
                    value,
                    SchemaBuilder.STRING_SCHEMA,
                    "value",
                    0
            );
        } else {
            sinkRecord = new SinkRecord(
                    "topic",
                    0,
                    SchemaBuilder.STRING_SCHEMA,
                    "key",
                    valueSchema,
                    value,
                    0
            );
        }

        ConnectRecord transformedRecord = maskJsonField.apply(sinkRecord);

        Struct data = isKey ? (Struct)transformedRecord.key(): (Struct)transformedRecord.value();
        String documentJson = data.getString("document");
        assertStringValue(documentJson, "/ssn", "");
    }

    @Test
    public void testNestedFieldInConnectStruct() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField.Value();
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

        Assertions.assertTrue(targetNode.isTextual() && targetNode.textValue().contentEquals(value), String.format("%s in %s = \"%s\" Actual=\"%s\"", path, jsonPayload, value, targetNode.textValue()));
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

    @Test
    public void classLoadTest() throws ClassNotFoundException {
        Class clazz = Class.forName("io.github.ferozed.kafka.connect.transforms.MaskJsonField$Value");
    }
}
