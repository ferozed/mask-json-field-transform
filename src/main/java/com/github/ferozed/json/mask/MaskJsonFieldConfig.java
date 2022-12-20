package com.github.ferozed.json.mask;

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

public class MaskJsonFieldConfig extends AbstractConfig {

    public static final String OUTER_FIELD_PATH = "OUTER_FIELD_PATH";
    public static final String MASK_FIELD_NAME = "MASK_FIELD_NAME";
    public static final String CONNECT_FIELD_NAME = "CONNECT_FIELD_NAME";

    /**
     * Construct a configuration with a ConfigDef and the configuration properties,
     * which can include properties for zero or more {@link ConfigProvider}
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
                        ConfigKeyBuilder.of(OUTER_FIELD_PATH, ConfigDef.Type.STRING)
                                .documentation("JsonPointer to outer field")
                                .defaultValue(Schema.Type.STRING.toString())
                                .validator(new ConfigDef.NonNullValidator())
                                .importance(ConfigDef.Importance.HIGH)
                                .build()
                )
                .define(
                        ConfigKeyBuilder.of(MASK_FIELD_NAME, ConfigDef.Type.STRING)
                                .documentation("Name of the field to be masked.")
                                .defaultValue(Schema.Type.STRING.toString())
                                .validator(new ConfigDef.NonEmptyString())
                                .importance(ConfigDef.Importance.HIGH)
                                .build()
                )
                .define(
                        ConfigKeyBuilder.of(CONNECT_FIELD_NAME, ConfigDef.Type.STRING)
                                .documentation("Connect field that has the json string")
                                .validator(new ConfigDef.NonEmptyString())
                                .importance(ConfigDef.Importance.HIGH)
                                .build()
                );
    }
}
