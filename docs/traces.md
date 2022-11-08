# Traces Arrow Schema

OTLP Traces are represented with the following Arrow Schema.

```
resource_spans: list<struct<
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
  scope_spans: list<struct<
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
    spans: list<item: struct<
      - start_time_unix_nano: uint64 
      - end_time_unix_nano: uint64
      - trace_id: 16_bytes_binary | 16_bytes_binary_dictionary 
      - span_id: 8_bytes_binary | 8_bytes_binary_dictionary 
      - trace_state: string | string_dictionary 
      - parent_span_id: 8_bytes_binary | 8_bytes_binary_dictionary 
      - name: string | string_dictionary,
      - kind: int32 
      - attributes: map<
          - key: string | string_dictionary 
          - sparse_union<
            - str: string | string_dictionary 
            - i64: int64 
            - f64: float64 
            - bool: bool 
            - binary: binary | binary_dictionary
          >
      >, 
      - dropped_attributes_count: uint32 
      - events: list<struct<
        - time_unix_nano: uint64 
        - name: string | string_dictionary 
        - attributes: map<
          - key: string | string_dictionary 
          - sparse_union<
            - str: string | string_dictionary 
            - i64: int64
            - f64: float64 
            - bool: bool 
            - binary: binary | binary_dictionary
          >
        >
        dropped_attributes_count: uint32
      >> 
      - dropped_events_count: uint32 
      - links: list<struct<
        - trace_id: 16_bytes_binary | 16_bytes_binary_dictionary 
        - span_id: 8_bytes_binary | 8_bytes_binary_dictionary
        - trace_state: string | string_dictionary 
        - attributes: map<
          - key: string | string_dictionary 
          - value: sparse_union<
            - str: string | string_dictionary 
            - i64: int64
            - f64: float64 
            - bool: bool 
            - binary: binary | binary_dictionary
      >> 
      - dropped_attributes_count: uint32 
      - dropped_links_count: uint32
      - status: struct<
        - code: int32 
        - status_message: string | string_dictionary
      >
    >
  >
>
```

Attributes are represented as a map of key-value pairs. The key is a dictionary encoded string, and the value is a
sparse union of string, int64, float64, bool, and binary. The dictionary encoding is used to reduce the size of the
payload. The sparse union is used to represent the value type. 

Events and links are represented as lists of structs. 