# New Versions

All new work should be done in a feature branch, against the next snapshot build.

# Releasing

- Remove the `-SNAPSHOT` qualifier from the version string.
- Publish to maven ( see instructions below )
- Merge to master.


# Publish To Maven

## Prerequisites

Publication can only be done by the Owner.

```bash
./gradlew clean build publish
```


# Testing

## Prerequisites

This was tested on a Confluent quickstart distribution downloaded from Confluent
website.

## Connector Configuration

This was tested using a simple FileSink connector:

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
    "transforms.mask_json_field.type": "com.github.ferozed.json.mask.MaskJsonField",
    "transforms.mask_json_field.OUTER_FIELD_PATH": "",
    "transforms.mask_json_field.MASK_FIELD_NAME": "ssn",
    "transforms.mask_json_field.CONNECT_FIELD_NAME": "document",

    "errors.tolerance": "all"

}
```

## Deploy connector

```bash
curl -X PUT -H "content-type: application/json" http://localhost:8083/connectors/json-mask-test/config -d @/home/ferozed/file-sink.json
```

## Produce JSON Records

```bash
./confluent-7.0.0/bin/kafka-console-producer --bootstrap-server localhost:9092 --topic json-topic

{ "name": "jon", "ssn": "111-22-3333" }
```

## Validate the output

Look at the `/tmp/file-sink.txt` file and make sure that the output is correct.