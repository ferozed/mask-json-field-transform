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

[JsonPointer](https://www.rfc-editor.org/rfc/rfc6901) path to the outer field in json payload

> OUTER_FIELD_PATH should be set to empty string, if the field is at root level.

*MASK_FIELD_NAME*

Name of the field whose value needs to be masked

*CONNECT_FIELD_NAME*

The name of the field in the connect record from which the JSON payload needs to be masked

> CONNECT_FIELD_NAME is optional. It is only specified for struct types, i.e when the data is in AVRO format.
> 
> If the data is in STRING format, then it is not used. In that case it should be set to empty sring.

# Examples

## String Examples

The following examples show usage when the JSON data is encoded as a string
in the connect record, without using AVRO.

> Also, note that the INPUT and OUTPUT are shown as parsed json,
> but in reality, when the transform gets the data, it will be 
> a JSON encoded string.

### Remove value from toplevel JSON field

*Kafka Connector Config*


```
"transforms": "mask_json_field",
"transforms.mask_json_field.type": "com.github.ferozed.json.mask.MaskJsonField",
"transforms.mask_json_field.OUTER_FIELD_PATH": "",
"transforms.mask_json_field.MASK_FIELD_NAME": "ssn",
"transforms.mask_json_field.CONNECT_FIELD_NAME": ""
```

*INPUT*

```
{ "name": "jon", "ssn": "111-22-1212" }
```

*OUTPUT*

```
{ "name": "jon", "ssn": "" }
```


### Remove value from nested JSON field

*Kafka Connector Config*

```
"transforms": "mask_json_field",
"transforms.mask_json_field.type": "com.github.ferozed.json.mask.MaskJsonField",
"transforms.mask_json_field.OUTER_FIELD_PATH": "/user_info",
"transforms.mask_json_field.MASK_FIELD_NAME": "ssn",
"transforms.mask_json_field.CONNECT_FIELD_NAME": ""
```

*INPUT*

```
{ 
   "id": 1,
   "title": "Manager",
   "user_info": {
       "name": "jon", 
       "ssn": "111-22-1212" 
   }
}
```

*OUTPUT*

```
{ 
   "id": 1,
   "title": "Manager",
   "user_info": {
       "name": "jon", 
       "ssn": "" 
   }
}
```

## Remove value from toplevel JSON field in a nested connect field


*Avro Schema of Connect Record*

```json
{
  "type": "record",
  "name": "employee",
  "namespace": "com.zillow.data",
  "fields": [
    {
      "name": "full_name",
      "type": "string"
    },
    {
      "name": "title",
      "type": "string"
    },
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "private_info",
      "type": "record",
      "fields": [
        {
          "name": "level",
          "type": "string"
        },
        {
          "name": "data",
          "type": "string"
        }
      ]
    }
  ]
}
```

*Kafka Connector Config*

```
"transforms": "mask_json_field",
"transforms.mask_json_field.type": "com.github.ferozed.json.mask.MaskJsonField",
"transforms.mask_json_field.OUTER_FIELD_PATH": "",
"transforms.mask_json_field.MASK_FIELD_NAME": "ssn",
"transforms.mask_json_field.CONNECT_FIELD_NAME": "private_info.data"
```

*INPUT*

```
{ 
   "id": 1,
   "title": "Manager",
   "full_name": "john doe",
   "private_info": {
       "level": "M1", 
       "data": "{\"city\": \"seattle\", \"ssn\": \"111-22-3333\"}" 
   }
}
```

*OUTPUT*

```
{ 
   "id": 1,
   "title": "Manager",
   "full_name": "john doe",
   "private_info": {
       "level": "M1", 
       "data": "{\"city\": \"seattle\", \"ssn\": \"\"}" 
   }
}
```