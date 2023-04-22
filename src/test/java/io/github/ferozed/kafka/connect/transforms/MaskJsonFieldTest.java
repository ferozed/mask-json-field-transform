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
import com.fasterxml.jackson.databind.node.*;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class MaskJsonFieldTest {

    ObjectMapper mapper = new ObjectMapper();

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

    @Test
    public void testToplevelFieldInConnectKeyStruct() throws JsonProcessingException {
        testToplevelFieldInConnectStructLib(true, "111-22-3333", "");
    }

    @Test
    public void testToplevelFieldInConnectValueStruct() throws JsonProcessingException {
        testToplevelFieldInConnectStructLib(false, "111-22-3333", "");
    }

    @Test
    public void testToplevelFieldInConnectValueStructInt() throws JsonProcessingException {
        testToplevelFieldInConnectStructLib(false, 1234, 0);
    }

    @Test
    public void testToplevelFieldInConnectValueStructBigIntMax() throws IOException {
        testLib(
                "{\"name\":\"john\",\"ssn\":" + Long.valueOf(Long.MAX_VALUE).toString() + "}",
                "/ssn",
                -1L,
                "{\"name\":\"john\",\"ssn\":-1}"
        );
    }

    @Test
    public void testToplevelFieldInConnectValueStructFloat() throws JsonProcessingException {
        testToplevelFieldInConnectStructLib(false, 1234.0, 0.0);
    }

    @Test
    private void testToplevelFieldInConnectStructLib(boolean isKey, Object valueToReplace, Object expectedReplacement) throws JsonProcessingException {
        MaskJsonField maskJsonField = isKey ? new MaskJsonField.Key() : new MaskJsonField.Value();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.CONNECT_FIELD_NAME, "document",
                        MaskJsonFieldConfig.REPLACEMENT_FIELD_PATH, "/ssn"
                )
        );

        Schema documentSchema = SchemaBuilder.struct()
                .field("document", Schema.STRING_SCHEMA)
                .schema();

        Schema valueSchema = SchemaBuilder.struct()
                .field("database", Schema.STRING_SCHEMA)
                .field("document", Schema.STRING_SCHEMA
                ).schema();

        String jsonPayloadTemplate = "{\"first_name\": \"john\", \"last_name\": \"doe\", \"ssn\": VALUE_TO_REPLACE}";
        String jsonDocument = "";
        if (valueToReplace.getClass() == String.class) {
            jsonDocument = jsonPayloadTemplate.replace("VALUE_TO_REPLACE", "\"" + ((String) valueToReplace) + "\"");
        } else {
            jsonDocument = jsonPayloadTemplate.replace("VALUE_TO_REPLACE", valueToReplace.toString());
        }
        Struct value = new Struct(valueSchema)
                .put("database", "dynamo")
                .put("document", jsonDocument);

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
        assertValue(documentJson, "/ssn", expectedReplacement);
    }

    @Test
    public void testNestedFieldInConnectStruct() throws JsonProcessingException {
        MaskJsonField maskJsonField = new MaskJsonField.Value();
        maskJsonField.configure(
                ImmutableMap.of(
                        MaskJsonFieldConfig.CONNECT_FIELD_NAME, "inner.document",
                        MaskJsonFieldConfig.REPLACEMENT_FIELD_PATH, "/ssn"
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
        assertValue(documentJson, "/ssn", "");
    }

    void assertValue(String jsonPayload, String path, Object value) throws JsonProcessingException {
        JsonNode root = mapper.readTree(jsonPayload);
        JsonPointer pointer = JsonPointer.compile(path);
        JsonNode targetNode = root.at(pointer);

        if (value.getClass() == String.class) {
            Assertions.assertTrue(targetNode.isTextual() && targetNode.textValue().equals(value), String.format("%s in %s = \"%s\" Actual=\"%s\"", path, jsonPayload, value, targetNode.textValue()));
        } else if (value.getClass() == Integer.class) {
            Assertions.assertTrue(Integer.valueOf(targetNode.intValue()).equals(value), String.format("%s in %s = \"%s\" Actual=\"%s\"", path, jsonPayload, value, targetNode.textValue()));
        } else if (value.getClass() == Double.class) {
            Assertions.assertTrue( Double.valueOf(targetNode.doubleValue()).equals(value), String.format("%s in %s = \"%s\" Actual=\"%s\"", path, jsonPayload, value, targetNode.textValue()));
        } else if (value.getClass() == Float.class) {
            Assertions.assertTrue(Float.valueOf(targetNode.floatValue()).equals(value), String.format("%s in %s = \"%s\" Actual=\"%s\"", path, jsonPayload, value, targetNode.textValue()));
        }
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

    @Test
    public void testJsonPointer() throws Exception {
        String payload = "{\"ssn\": \"111\"}";

        JsonPointer pointer = JsonPointer.compile("/ssn");

        JsonNode root = mapper.readTree(payload);

        JsonNode targetNode = root.at(pointer);
        JsonNode parentNode = root.at(pointer.head());

        if (parentNode.isObject()) {
            ((ObjectNode)parentNode).set(pointer.getMatchingProperty(), TextNode.valueOf(""));
        }

        System.out.println(mapper.writeValueAsString(root));
    }

    @Test
    public void arrayTests() throws IOException {
        testLib(
                "{\"ssn\": \"111\"}",
                "/ssn",
                "",
                "{\"ssn\":\"\"}"
        );

        testLib(
                "{\"ssn\": \"111\"}",
                "/ssn",
                "_REDACTED_",
                "{\"ssn\":\"_REDACTED_\"}"
        );

        // REPLACE WHOLE ARRAY
        testLib(
                "{\"ssn\": [\"111\",\"22\",\"3333\" ]}",
                "/ssn",
                "_REDACTED_",
                "{\"ssn\":[]}"
        );

        testLib(
                "{\"ssn\": [\"111\",\"22\",\"3333\" ]}",
                "/ssn/2",
                "_REDACTED_",
                "{\"ssn\":[\"111\",\"22\",\"_REDACTED_\"]}"
        );

        // array in deep struct
        testLib(
                "{\"foo\":{\"bar\":{\"ssn\":[\"111\",\"22\",\"3333\"]}}}",
                "/foo/bar/ssn/2",
                "_REDACTED_",
                "{\"foo\":{\"bar\":{\"ssn\":[\"111\",\"22\",\"_REDACTED_\"]}}}"
        );

        // array of int in deep struct
        testLib(
                "{\"foo\":{\"bar\":{\"ssn\":[111,22,3333]}}}",
                "/foo/bar/ssn/2",
                -1,
                "{\"foo\":{\"bar\":{\"ssn\":[111,22,-1]}}}"
        );

        // array of float in deep struct
        testLib(
                "{\"oo\":{\"bar\":{\"ssn\":[111.1,22.2,3333.2]}}}",
                "/oo/bar/ssn/2",
                -1.1,
                "{\"oo\":{\"bar\":{\"ssn\":[111.1,22.2,-1.1]}}}"
        );

    }

    private void testLib(
            String payload,
            String path,
            Object replacement,
            String expectedJson
    ) throws IOException {
        MaskJsonField maskJsonField = new MaskJsonField.Value();

        Map<String,Object> configs = new HashMap<>();

        configs.put(MaskJsonFieldConfig.CONNECT_FIELD_NAME, "inner.document");
        configs.put(MaskJsonFieldConfig.REPLACEMENT_FIELD_PATH, path);

        if (replacement.getClass() == Integer.class) {
            configs.put(MaskJsonFieldConfig.REPLACEMENT_VALUE_INT, (Integer)replacement);
        }
        else if (replacement.getClass() == Long.class) {
            configs.put(MaskJsonFieldConfig.REPLACEMENT_VALUE_LONG, (Long)replacement);
        }
        else if (replacement.getClass() == Float.class) {
            configs.put(MaskJsonFieldConfig.REPLACEMENT_VALUE_DOUBLE, Double.valueOf((Float)replacement));
        }
        else if (replacement.getClass() == Double.class) {
            configs.put(MaskJsonFieldConfig.REPLACEMENT_VALUE_DOUBLE, (Double)replacement);
        }
        else if (replacement.getClass() == String.class) {
            configs.put(MaskJsonFieldConfig.REPLACEMENT_VALUE_STRING, (String)replacement);
        } else {
            throw new RuntimeException("Unsupported value type: " + replacement.getClass());
        }

        maskJsonField.configure(configs);

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
                                .put("document", payload)
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
        String replacedJson = data.getStruct("inner").getString("document");
        //assertValue(replacedJson, path, replacement);

        Assertions.assertEquals(expectedJson, replacedJson);
    }
    @Test
    public void jsonPointerReplaceIntTest() throws IOException {
        replaceWithPointerLib(
                "{\"ssn\": \"111\"}",
                "/ssn",
                "",
                "{\"ssn\":\"\"}"
        );

        replaceWithPointerLib(
                "{\"ssn\": \"111\"}",
                "/ssn",
                "_REDACTED_",
                "{\"ssn\":\"_REDACTED_\"}"
        );

        // REPLACE WHOLE ARRAY
        replaceWithPointerLib(
                "{\"ssn\": [\"111\",\"22\",\"3333\" ]}",
                "/ssn",
                "_REDACTED_",
                "{\"ssn\":[]}"
        );

        replaceWithPointerLib(
                "{\"ssn\": [\"111\",\"22\",\"3333\" ]}",
                "/ssn/2",
                "_REDACTED_",
                "{\"ssn\":[\"111\",\"22\",\"_REDACTED_\"]}"
        );
    }

    private void replaceWithPointerLib(
            String payload,
            String path,
            Object replacement,
            String expectedJson
    ) throws IOException {
        JsonNode replacementNode =
                replaceWithPointer(
                        payload,
                        path,
                        replacement
                );

        String replacedJson = mapper.writeValueAsString(replacementNode);

        Assertions.assertEquals(expectedJson, replacedJson);
    }

    private JsonNode replaceWithPointer(
            String payload,
            String path,
            Object replacement
    ) throws IOException {
        JsonPointer pointer = JsonPointer.compile(path);

        JsonNode root = mapper.readTree(payload);

        JsonNode targetNode = root.at(pointer);
        JsonNode parentNode = root.at(pointer.head());

        JsonNode replacementNode = null;
        if (targetNode.isTextual()) {
            replacementNode = TextNode.valueOf(replacement.toString());
        } else if (targetNode.isInt()) {
            replacementNode = IntNode.valueOf((Integer)replacement);
        } else if (targetNode.isBigInteger()) {
            replacementNode = BigIntegerNode.valueOf((BigInteger)replacement);
        } else if (targetNode.isFloat()) {
            replacementNode = FloatNode.valueOf((Float) replacement);
        } else if (targetNode.isDouble()) {
            replacementNode = DoubleNode.valueOf((Double) replacement);
        } else if (targetNode.isArray()) {
            replacementNode = mapper.createArrayNode();
        }

        if (parentNode.isObject()) {
            ((ObjectNode)parentNode).set(pointer.getMatchingProperty(), replacementNode);
        } else if (parentNode.isArray()) {
            ((ArrayNode)parentNode).set(pointer.tail().getMatchingIndex(), replacementNode);
        }

        return root;
    }
}
