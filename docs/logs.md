# Logs Arrow Schema

OTLP Logs are represented with the following Arrow Schema.

```
resource_logs: list<struct<
  - resource: struct<
    - attributes: map<
      - key: string | string_dictionary 
      - value: sparse_union<
        - str: string | string_dictionary 
        - i64: int64
        - f64: float64 
        - bool: bool
        - binary: binary | binary_dictionary
      >
    >
    dropped_attributes_count: uint32
  > 
  schema_url: string | string_dictionary 
  scope_logs: list<struct<
    - scope: struct<
      - name: string | string_dictionary 
      - version: string | string_dictionary 
      - attributes: map<
        - key: string | string_dictionary 
        - value: sparse_union<
          - str: string | string_dictionary 
          - i64: int64 
          - f64: float64 
          - bool: bool 
          - binary: binary | binary_dictionary
        >
      > 
      dropped_attributes_count: uint32
    >, 
    schema_url: string | string_dictionary 
    logs: list<item: struct<
      - time_unix_nano: uint64 
      - observed_time_unix_nano: uint64 
      - trace_id: 16_bytes_binary | 16_bytes_binary_dictionary 
      - span_id: 8_bytes_binary | 8_bytes_binary_dictionary
      - severity_number: int32 
      - severity_text: string | string_dictionary 
      - body: sparse_union<
        - str: string | string_dictionary 
        - i64: int64 
        - f64: float64 
        - bool: bool 
        - binary: binary | binary_dictionary
      >
      attributes: map<
        - string | string_dictionary 
        - sparse_union<
          - str: string | string_dictionary 
          - i64: int64
          - f64: float64 
          - bool: bool 
          - binary: binary | binary_dictionary
        >
      > 
      dropped_attributes_count: uint32 
      flags: uint32
    >
  >
>
```

Attributes are represented as a map of key-value pairs. The key is a dictionary encoded string, and the value is a 
sparse union of string, int64, float64, bool, and binary. The dictionary encoding is used to reduce the size of the 
payload. The sparse union is used to represent the value type. 