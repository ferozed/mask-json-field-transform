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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class MaskJsonFieldTest extends BaseTests {
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

    @Test
    public void mapTest() {
        Map<String,Object> map = new HashMap<>();
        map.put("string", "string");
        map.put("int", 1);
        String [] array_string = new String[2];
        array_string[0] = "first_string";
        array_string[1] = "secibd_string";
        map.put("array_string", array_string);
        Integer [] array_int = new Integer[2];
        array_int[0] = 100;
        array_int[1] = 200;
        map.put("array_int", array_int);

        try {
            String output = mapper.writeValueAsString(map);
            System.out.println(output);

            JsonNode node = mapper.readTree(output);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
}
