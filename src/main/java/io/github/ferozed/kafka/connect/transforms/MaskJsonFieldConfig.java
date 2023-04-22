/**
 * Copyright © Feroze Daud (ferozed DOT oss AT gmail.com)
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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

public class MaskJsonFieldConfig extends AbstractConfig {

    public static final String REPLACEMENT_FIELD_PATH = "REPLACEMENT_FIELD_PATH";
    public static final String CONNECT_FIELD_NAME = "CONNECT_FIELD_NAME";

    public static final String REPLACEMENT_VALUE_STRING = "REPLACEMENT_VALUE_STRING";
    public static final String REPLACEMENT_VALUE_INT = "REPLACEMENT_VALUE_INT";
    public static final String REPLACEMENT_VALUE_LONG = "REPLACEMENT_VALUE_LONG";
    public static final String REPLACEMENT_VALUE_DOUBLE = "REPLACEMENT_VALUE_DOUBLE";

    /**
     * Construct a configuration with a ConfigDef and the configuration properties,
     * which can include properties for zero or more {@link ConfigDef}
     * that will be used to resolve variables in configuration property values.
     *
     * @param definition the definition of the configurations; may not be null
     * @param originals  the configuration properties plus any optional config provider properties; may not be null
     */
    public MaskJsonFieldConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(
                        ConfigKeyBuilder.of(REPLACEMENT_FIELD_PATH, ConfigDef.Type.STRING)
                                .documentation("JsonPointer to replacement field")
                                .defaultValue(Schema.Type.STRING.toString())
                                .validator(new ConfigDef.NonNullValidator())
                                .importance(ConfigDef.Importance.HIGH)
                                .build()
                )
                .define(
                        ConfigKeyBuilder.of(CONNECT_FIELD_NAME, ConfigDef.Type.STRING)
                                .documentation("Connect field that has the json string")
                                .importance(ConfigDef.Importance.LOW)
                                .defaultValue("")
                                .build()
                )
                .define(
                        ConfigKeyBuilder.of(REPLACEMENT_VALUE_STRING, ConfigDef.Type.STRING)
                                .documentation("The string value that should be used to replace.")
                                .importance(ConfigDef.Importance.LOW)
                                .defaultValue("")
                                .build()
                )
                .define(
                        ConfigKeyBuilder.of(REPLACEMENT_VALUE_INT, ConfigDef.Type.INT)
                                .documentation("The string value that should be used to replace.")
                                .importance(ConfigDef.Importance.LOW)
                                .defaultValue(0)
                                .build()
                )
                .define(
                        ConfigKeyBuilder.of(REPLACEMENT_VALUE_LONG, ConfigDef.Type.LONG)
                                .documentation("The long value that should be used to replace.")
                                .importance(ConfigDef.Importance.LOW)
                                .defaultValue(0L)
                                .build()
                )
                .define(
                        ConfigKeyBuilder.of(REPLACEMENT_VALUE_DOUBLE, ConfigDef.Type.DOUBLE)
                                .documentation("The long value that should be used to replace.")
                                .importance(ConfigDef.Importance.LOW)
                                .defaultValue(0.0)
                                .build()
                )
                ;
    }
}
