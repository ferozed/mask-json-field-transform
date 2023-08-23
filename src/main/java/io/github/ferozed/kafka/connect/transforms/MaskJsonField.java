/**
 * Copyright © 2023 Feroze Daud (ferozed DOT oss AT gmail.com)
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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.github.jcustenborder.kafka.connect.transform.common.BaseTransformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.github.ferozed.kafka.connect.transforms.MaskJsonFieldConfig.*;

public class MaskJsonField<R extends ConnectRecord<R>> extends BaseTransformation<R> {
    MaskJsonFieldConfig config;
    String replacementFieldPath;
    String connectFieldName;

    private Boolean isKey;

    static final ObjectMapper mapper = new ObjectMapper();

    private MaskJsonField(Boolean isKey) {
        this.isKey = isKey;
    }

    /**
     * Apply transformation to the {@code record} and return another record object (which may be {@code record} itself) or {@code null},
     * corresponding to a map or filter operation respectively.
     * <p>
     * A transformation must not mutate objects reachable from the given {@code record}
     * (including, but not limited to, {@link Headers Headers},
     * {@link Struct Structs}, {@code Lists}, and {@code Maps}).
     * If such objects need to be changed, a new ConnectRecord should be created and returned.
     * <p>
     * The implementation must be thread-safe.
     *
     * @param r Connect Record
     */
    @Override
    public R apply(R r) {

        if (isKey) {
            final SchemaAndValue transformed = process(r, r.keySchema(), r.key());

            return r.newRecord(
                    r.topic(),
                    r.kafkaPartition(),
                    transformed.schema(),
                    transformed.value(),
                    r.valueSchema(),
                    r.value(),
                    r.timestamp()
            );
        } else {
            final SchemaAndValue transformed = process(r, r.valueSchema(), r.value());

            return r.newRecord(
                    r.topic(),
                    r.kafkaPartition(),
                    r.keySchema(),
                    r.key(),
                    transformed.schema(),
                    transformed.value(),
                    r.timestamp()
            );
        }
    }

    /**
     * Configuration specification for this transformation.
     **/
    @Override
    public ConfigDef config() {
        return MaskJsonFieldConfig.config();
    }

    /**
     * Signal that this transformation instance will no longer will be used.
     **/
    @Override
    public void close() {

    }

    /**
     * Configure this class with the given key-value pairs
     *
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {
        this.config = new MaskJsonFieldConfig(MaskJsonFieldConfig.config(), configs);
        this.replacementFieldPath = this.config.getString(MaskJsonFieldConfig.REPLACEMENT_FIELD_PATH);
        this.connectFieldName = this.config.getString(CONNECT_FIELD_NAME);
    }

    @Override
    protected SchemaAndValue processString(ConnectRecord record, Schema inputSchema, String input) {
        if (isKey) {
            String value = (String) record.key();

            Schema valueSchema = record.keySchema();

            String replacementString = replaceJsonWithPath(value, this.replacementFieldPath);

            return new SchemaAndValue(Schema.STRING_SCHEMA, replacementString);
        } else {
            String value = (String) record.value();

            Schema valueSchema = record.valueSchema();

            String replacementString = replaceJsonWithPath(value, this.replacementFieldPath);

            return new SchemaAndValue(Schema.STRING_SCHEMA, replacementString);
        }

    }

    @Override
    protected SchemaAndValue processStruct(ConnectRecord record, Schema inputSchema, Struct input) {
        // get the json serialized field from connect record.

        String [] tokens = connectFieldName.split("\\.");
        String json = null;
        Struct struct = input;
        for(int i = 0; i < tokens.length; i++) {
            String field = tokens[i];

            if (i == tokens.length - 1) {
                json = struct.getString(field);
                String replacement = replaceJsonWithPath(json, this.replacementFieldPath);
                struct.put(field, replacement);
            } else {
                struct = struct.getStruct(field);
            }
        }

        if (json == null || json.trim().length() ==0) {
            return new SchemaAndValue(inputSchema, input);
        }

        return new SchemaAndValue(inputSchema, input);
    }

    private String replaceJsonWithPath(
            String payload,
            String path
    ) {
        try {
            JsonNode replacementNode = replaceWithPointer(payload, path);
            return mapper.writeValueAsString(replacementNode);
        } catch (IOException e) {
            return payload;
        }
    }

    private JsonNode replaceWithPointer(
            String payload,
            String path
    ) throws IOException {
        JsonPointer pointer = JsonPointer.compile(path);

        JsonNode root = null;
        root = mapper.readTree(payload);

        JsonNode targetNode = root.at(pointer);

        if(targetNode.isMissingNode()) {
            throw new IOException("Pointer did not match");
        }
        JsonNode parentNode = root.at(pointer.head());

        JsonNode replacementNode = null;
        if (targetNode.isTextual()) {
            replacementNode = TextNode.valueOf(config.getString(REPLACEMENT_VALUE_STRING));
        } else if (targetNode.isInt()) {
            replacementNode = IntNode.valueOf(config.getInt(REPLACEMENT_VALUE_INT));
        } else if (targetNode.isLong()) {
            replacementNode = LongNode.valueOf(config.getLong(REPLACEMENT_VALUE_LONG));
        } else if (targetNode.isBigInteger()) {
            BigInteger bi = BigInteger.valueOf(config.getInt(REPLACEMENT_VALUE_INT));
            replacementNode = BigIntegerNode.valueOf(bi);
        } else if (targetNode.isFloat()) {
            replacementNode = FloatNode.valueOf(config.getDouble(REPLACEMENT_VALUE_INT).floatValue());
        } else if (targetNode.isDouble()) {
            replacementNode = DoubleNode.valueOf(config.getDouble(REPLACEMENT_VALUE_DOUBLE));
        } else if (targetNode.isArray()) {
            replacementNode = mapper.createArrayNode();
        } else if (targetNode.isObject()) {
            replacementNode = mapper.createObjectNode();
        }

        if (parentNode.isObject()) {
            ((ObjectNode)parentNode).set(pointer.last().getMatchingProperty(), replacementNode);
        } else if (parentNode.isArray()) {
            ((ArrayNode)parentNode).set(pointer.last().getMatchingIndex(), replacementNode);
        }

        return root;
    }

    /***
     * THis function handles the case when you have json data with no schema.
     * For eg, if you use `JsonConverter` with `schemas.enable=false`
     *
     * In this case, the `schema` field in connect record will be null,
     * and the value field will be a %lt;String,Object&gt;
     *
     * TODO:
     *     Need to add support for embedded json strings in the map.
     *     in that case, use `connect.field.name` to get the embedded json
     *     from the map. Then apply logic on that string.
     * @param record
     * @param input
     * @return
     */
    @Override
    protected SchemaAndValue processMap(R record, Map<String, Object> value) {
        Map<String, Object> input = value;
        String [] tokens = connectFieldName.split("\\.");
        try {
            for(int i = 0; i < tokens.length; i++) {
                String field = tokens[i];

                if (i == tokens.length - 1) {
                    String json = (String)input.get(field);
                    JsonNode replacement = replaceWithPointer(json, this.replacementFieldPath);
                    input.put(field, mapper.writeValueAsString(replacement));
                } else {
                    input = (Map<String,Object>)input.get(field);
                }
            }
        } catch (JsonProcessingException e) {
            //throw new RuntimeException(e);
        } catch (IOException e) {
            //throw new RuntimeException(e);
        }

        return new SchemaAndValue(
                isKey ? record.keySchema() : record.valueSchema(),
                value);
    }

    public static class Key<R extends ConnectRecord<R>> extends MaskJsonField<R> {
        public Key() {
            super(true);
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends MaskJsonField<R> {
        public Value() {
            super(false);
        }
    }

    interface IPropertyGetter {
        String getString(String field);

        public class StructPropertyGetter implements IPropertyGetter {

            Struct struct;
            public StructPropertyGetter(Struct struct) {
                this.struct = struct;
            }

            @Override
            public String getString(String field) {
                return this.struct.getString(field);
            }
        }

        public class MapPropertyGetter implements IPropertyGetter {

            Map<String,String> map;
            public MapPropertyGetter(Map<String,String> map) {
                this.map = map;
            }
            @Override
            public String getString(String field) {
                return map.get(field);
            }
        }
    }

}
