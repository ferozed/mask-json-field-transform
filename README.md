# Introduction

This jar contains a Kafka Connect Single Message Transform (SMT) implementation.

## MaskJsonField

`MaskJsonField` transform will read a JSON payload from a connect field.

It will then mask out a field from the payload and return it.

For eg:

*INPUT*

```
{ "name": "jon", "ssn": "111-22-1212" }
```

*OUTPUT*

```
{ "name": "jon", "ssn": "" }
```

### Configuration

*OUTER_FIELD_PATH*

JsonPointer path to the outer field in json payload

*MASK_FIELD_NAME*

Name of the field whose value needs to be masked

*CONNECT_FIELD_NAME*

The name of the field in the connect record from which the JSON payload needs to be masked |






