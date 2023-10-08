package io.github.ferozed.kafka.connect.transforms;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class tests the SMT with a map input.
 * Usually, this is relevant in the case of Converters that dont have a schema to work with.
 * in that case, they will translate the input from the topic into a map.
 */
public class MapTests extends BaseTests {

    @Test
    public void testMapSimpleString() throws IOException
    {
        testLib(
                false,
                "document",
                "{\"ssn\": \"111-22-3333\"}",
                "/ssn",
                "xxx-xx-xxxx",
                "{\"ssn\":\"xxx-xx-xxxx\"}"
        );
    }

    @Test
    public void testMapSimpleInt() throws IOException {
        testLib(
                false,
                "document",
                "{\"ssn\":1}",
                "/ssn",
                1,
                "{\"ssn\":1}"
        );
    }

    @Test
    public void testMapSimpleLong() throws IOException
    {
        /*
        NOTE: In the case of longs, if the number in the json looks like an integer,
        then we will use REPLACEMENT_VALUE_INT to replace the value, instead of REPLACEMENT_VALUE_LONG
        This is because there is no way to indicate in a json payload whether the number is an int or long.
        That is, you cant do this: {"ssn": 1L}
         */
        testLib(
                false,
                "document",
                "{\"ssn\":1}",
                "/ssn",
                100,
                "{\"ssn\":100}"
        );
    }

    @Test
    public void testMapFloat() throws IOException
    {
        testLib(
                false,
                "document",
                "{\"ssn\":1.23}",
                "/ssn",
                0.1,
                "{\"ssn\":0.1}"
        );
    }

    @Test
    public void arayTests() throws IOException
    {
        testLib(
                false,
                "document",
                "{\"ssn\":[111,22,333]}",
                "/ssn/0",
                0,
                "{\"ssn\":[0,22,333]}"
        );

        /*
        Currently, the only way to enable clearing of an array
        is to set a replacement_value of a different type
         */
        testLib(
                false,
                "document",
                "{\"ssn\":[111,22,333]}",
                "/ssn",
                "_REDACTED_",
                "{\"ssn\":[]}"
        );

        testLib(
                false,
                "inner.document",
                "{\"ssn\":[111,22,333]}",
                "/ssn/0",
                0,
                "{\"ssn\":[0,22,333]}"
        );

        testLib(
                false,
                "inner.document",
                "{\"ssn\":[[111,22,333]]}",
                "/ssn/0/0",
                0,
                "{\"ssn\":[[0,22,333]]}"
        );
    }


    @Test
    public void objectTests() throws IOException
    {

        /*
        Currently, the only way to enable clearing of an array or object
        is to set a replacement_value of a different type
         */
        testLib(
                false,
                "document",
                "{\"ssn\":{\"value\": \"111-22-3333\", \"safeForDisplay\":\"xxx-xx-3333\"}}",
                "/ssn",
                "_REDACTED_",
                "{\"ssn\":{}}"
        );

        testLib(
                false,
                "document",
                "{\"ssn\":{\"value\": \"111-22-3333\", \"safeForDisplay\":\"xxx-xx-3333\"}}",
                "/ssn/value",
                "",
                "{\"ssn\":{\"value\":\"\",\"safeForDisplay\":\"xxx-xx-3333\"}}"
        );
    }

    private void testLib(
            boolean isKey,
            String connectFieldName,
            String payload,
            String path,
            Object replacement,
            String expectedJson
    ) throws IOException {
        MaskJsonField maskJsonField = isKey ? new MaskJsonField.Key() : new MaskJsonField.Value();

        Map<String,Object> configs = new HashMap<>();

        configs.put(MaskJsonFieldConfig.CONNECT_FIELD_NAME, connectFieldName);
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

        ObjectNode rootNode = mapper.createObjectNode();
        ObjectNode current = rootNode;
        String [] tokens = connectFieldName.split("\\.");
        for(int i=0; i < tokens.length; i++) {
            if (i == tokens.length -1) {
                current.put(tokens[i], payload);
            } else {
                ObjectNode node = mapper.createObjectNode();
                current.set(tokens[i], node);
                current = node;
            }
        }


        Map<String,Object> payloadAsMap = mapper.convertValue(rootNode, new TypeReference<Map<String, Object>>(){});

        SinkRecord sinkRecord = new SinkRecord(
                "topic",
                0,
                SchemaBuilder.STRING_SCHEMA,
                "key",
                null,
                payloadAsMap,
                0
        );

        ConnectRecord transformedRecord = maskJsonField.apply(sinkRecord);

        Map data = (Map)transformedRecord.value();
        String replacedJson = (String)getFieldFromMap(data, connectFieldName);

        Assertions.assertEquals(expectedJson, replacedJson);
    }

    private Object getFieldFromMap(
            Map<String,Object> map,
            String fieldPath
    ) {
        String[] tokens = fieldPath.split("\\.");
        Map<String,Object> current = map;
        for (int i=0; i < tokens.length; i++) {
            String field = tokens[i];
            Object value = current.get(field);
            if ( i == tokens.length-1) {
                return value;
            } else {
                current = (Map<String,Object>)map.get(field);
            }
        }

        return null;
    }
}
