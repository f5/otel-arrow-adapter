# Encoding/Decoding Validation Process

This document describes the validation process employed to confirm the correctness
and resilience of encoding and decoding process of OTLP entities to/from OTel Arrow
entities. The OTel Arrow protocol is designed to enhance the transport efficiency 
of ALL forms of telemetry data, including metrics, logs, and traces, in a seamless 
manner. The complexity and dynamic nature of the OTLP entities make it challenging
to validate the encoding/decoding process with traditional methods (traditional 
QA process, or unit tests). The approach followed by this project offers a more 
systematic and robust method.

## Automatic generation of metrics, logs, and traces

In this step we generate a large amount of random metrics, logs, and traces having 
different characteristics in terms of shape, size, and content. The goal is to
test the capacity of the encoding process to handle a wide range of diversity in
the data.

Mandatory fields will not be generated systematically in order to test the
robustness of the encoding/decoding process (the original Protobuf encoding 
doesn't enforce mandatory fields).

## Generic comparison of OTLP entities before and after encoding/decoding

OTLP entities are serialized in their JSON representation before encoding and 
after decoding. These two JSON representations must be logically equivalent.
This comparison is implemented by a dedicated function that recursively traverses
the 2 JSON trees and compares the values of the fields taking into account that 
the fields are not ordered and that the batching process may have changed the 
internal organization of the data. 

## Decoding of invalid data  

A generic process has been implemented to inject specific errors and data changes
in the encoded data. For example, the existing process keep the Arrow records
but randomly change their payload types. The goal is to test the resilience of
the decoding process to invalid data. The decoding layer must be able to handle
any invalid data and return appropriate error messages without crashing.

