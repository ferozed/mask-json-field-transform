# New Versions

All new work should be done in a feature branch, against the next snapshot build.

# Releasing

- Remove the `-SNAPSHOT` qualifier from the version string.
- Publish to maven ( see instructions below )
- Merge to master.


# Publish To Maven

## Prerequisites

Publication can only be done by the Owner. We first publish to sonatype staging repository,
and then promote to release repo manually.

## Publish to staging repo

```bash
./gradlew clean build publish
```

## Promote to release

Follow instructions in [Releasing](https://central.sonatype.org/publish/release/#deployment) section of OSSRH documentation.

# Testing

## Prerequisites

This was tested on a Confluent quickstart distribution downloaded from Confluent
website.

The following test scenarios use the confluent `FileStream` connectors. They are not enabled by default in the
confluent distributions.

To enable them:

1. Find your CONFLUENT_HOME directory. This is where your confluent distribution is unzipped/untarred.
2. Next, copy ${CONFLUENT_HOME}/share/filestream-connectors ${CONFLUENT_HOME}/share/java/filestream-connectors
3. Restart the connect server
4. Make sure that the connectors are installed by running `curl -s http://localhost:8083/connector-plugins` and
verifying that `FileSource` and `FileSink` connectors show up

# Test With Sink Connector

This was tested using a simple FileSink connector. The configurations depend on the converter implementation used

## StringConverter 

### Config

```json
{
    "name": "json-mask-test",
    "connector.class": "FileStreamSink",
    "file": "/tmp/file-sink.txt",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "tasks.max": 1,
    "topics": "json-topic",
    "transforms": "mask_json_field",
    "transforms.mask_json_field.type": "io.github.ferozed.kafka.connect.transforms.MaskJsonField$Value",
    "transforms.mask_json_field.REPLACEMENT_FIELD_PATH": "/ssn",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"

}
```

### Deploy connector

```bash
curl -X PUT \
-H "content-type: application/json" \
http://localhost:8083/connectors/json-mask-test/config \
-d @/home/ferozed/file-sink.json
```

### Produce JSON Records

```bash
./confluent-7.0.0/bin/kafka-console-producer \
--bootstrap-server \
localhost:9092 \
--topic json-topic

{ "name": "jon", "ssn": "111-22-3333" }
```

## JsonConverter

### Config
```json
{
    "name": "json-mask-test-json-converter",
    "connector.class": "FileStreamSink",
    "file": "/tmp/file-sink.txt",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "tasks.max": 1,
    "topics": "json-topic",
    "transforms": "mask_json_field",
    "transforms.mask_json_field.type": "io.github.ferozed.kafka.connect.transforms.MaskJsonField$Value",
    "transforms.mask_json_field.REPLACEMENT_FIELD_PATH": "/ssn",
    "transforms.mask_json_field.CONNECT_FIELD_NAME": "document",

    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true"

}
```

### Deploy connector

```bash
curl -X PUT \
-H "content-type: application/json" \
http://localhost:8083/connectors/json-mask-test/config \
-d @/home/ferozed/file-sink.json
```

### Produce JSON Records

```bash
./confluent-7.0.0/bin/kafka-console-producer \
--bootstrap-server \
localhost:9092 \
--topic json-topic

{"document": "{ \"name\": \"jon\", \"ssn\": \"111-22-3333\" }"}
```

## Validate the output

Look at the `/tmp/file-sink.txt` file and make sure that the output is correct.

# Test With Source Connector

## StringConverter

### Connector Configuration

This was tested using a simple FileStreamSource connector.

```json
{
  "name": "json-mask-test-source",
  "connector.class": "FileStreamSource",
  "file": "/tmp/json-input.txt",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.storage.StringConverter",
  "tasks.max": 1,
  "topic": "json-source-topic",
  "transforms": "mask_json_field",
  "transforms.mask_json_field.type": "io.github.ferozed.kafka.connect.transforms.MaskJsonField$Value",
  "transforms.mask_json_field.REPLACEMENT_FIELD_PATH": "/ssn",

  "errors.tolerance": "all",
  "errors.log.enable": "true",
  "errors.log.include.messages": "true"

}
```

### Deploy connector

```bash
curl -X PUT \
-H "content-type: application/json" \
http://localhost:8083/connectors/json-mask-test-source/config \
-d @/home/ferozed/file-source.json
```

### Produce JSON Records

```bash
for i in {1..1000}; \
do \
echo "{\"name\": \"jon-${i}\", \"ssn\": \"111-22-${i}\"}" >> /tmp/json-input.txt;\
done
```

### Consume from source topic

```bash
./confluent-7.0.0/bin/kafka-console-consumer \
--bootstrap-server \
localhost:9092 \
--topic json-source-topic \
--partition 0 \
--offset 0
```

## Validate the output

Make sure messages in the topic have `ssn` filed set to empty string. Example:

```json
{"name":"jon-996","ssn":""}
{"name":"jon-997","ssn":""}
{"name":"jon-998","ssn":""}
{"name":"jon-999","ssn":""}
{"name":"jon-1000","ssn":""}
```
