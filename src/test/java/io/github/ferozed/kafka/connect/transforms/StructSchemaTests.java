package io.github.ferozed.kafka.connect.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StructSchemaTests extends BaseTests {
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
}
