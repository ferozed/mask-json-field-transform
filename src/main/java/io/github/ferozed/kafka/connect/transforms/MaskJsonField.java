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
import com.github.jcustenborder.kafka.connect.transform.common.BaseTransformation;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;

import java.util.Map;

import static io.github.ferozed.kafka.connect.transforms.MaskJsonFieldConfig.CONNECT_FIELD_NAME;

public class MaskJsonField<R extends ConnectRecord<R>> extends BaseTransformation<R> {
    MaskJsonFieldConfig config;
    String outerFieldPath;
    String maskFieldName;
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
        this.outerFieldPath = this.config.getString(MaskJsonFieldConfig.OUTER_FIELD_PATH);
        this.maskFieldName = this.config.getString(MaskJsonFieldConfig.MASK_FIELD_NAME);
        this.connectFieldName = this.config.getString(CONNECT_FIELD_NAME);
    }

    @Override
    protected SchemaAndValue processString(ConnectRecord record, Schema inputSchema, String input) {
        if (isKey) {
            String value = (String) record.key();

            Schema valueSchema = record.keySchema();

            String replacementString = replaceKeyInJsonString(value);

            return new SchemaAndValue(Schema.STRING_SCHEMA, replacementString);
        } else {
            String value = (String) record.value();

            Schema valueSchema = record.valueSchema();

            String replacementString = replaceKeyInJsonString(value);

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
                String replacement = replaceKeyInJsonString(json);
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

    private String replaceKeyInJsonString(String jsonPayload) {
        JsonNode node = null;
        try {
            node = mapper.readTree(jsonPayload);
        } catch (JsonProcessingException e) {
            // error parsing json. return it as it is.
            return jsonPayload;
        }

        JsonPointer targetPointer = JsonPointer.compile(outerFieldPath);
        JsonNode target = node.at(targetPointer);

        // no match.
        if (target == null) {
            return jsonPayload;
        }

        if (target instanceof  ObjectNode) {
            ObjectNode o = (ObjectNode) target;
            // replace it only if the field already exists.
            if (o.has(this.maskFieldName)) {
                o.put(this.maskFieldName, "");
            }
        }

        String replacementString = null;
        try {
            replacementString = mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return replacementString;
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

}
