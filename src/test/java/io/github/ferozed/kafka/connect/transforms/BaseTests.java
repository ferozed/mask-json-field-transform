package io.github.ferozed.kafka.connect.transforms;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;

public abstract class BaseTests {

    protected ObjectMapper mapper = new ObjectMapper();
    protected void assertValue(String jsonPayload, String path, Object value) throws JsonProcessingException {
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
}
