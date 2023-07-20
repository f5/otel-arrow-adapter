First, we will generate a data file using the example configuration in
[`collector/examples/synthesize/record.yaml`](collector/examples/synthesize/record.yaml).
See step 1 in the [README](collector/examples/synthesize/README.md),
you will simply run the collector and use CTRL-C to stop the generator
after a while.  In this branch, I have removed the batch processor and
increased the rate of generation, so that requests are small and the
generator yields data quickly.

There are two ways to reproduce the currently-observed bug related to
decoding delta-encoded uint16 values in the consumer.

1. In `tools/trace_verify`, (as described in the [HOWTO](tools/trace_verify/HOWTO.md)), first copy a file named `recorded_traces.json` into the current working directory, and then run `go test .`.  (Or use a symlink e.g., `ln -s ../../collector/synthesize/recorded_traces.json .`).  Note this test uses `assert.Equiv`.
2. In `collector/examples/synthesize`, (as described in the [README](collector/examples/synthesize/README.md)), there are two commands to record and replay telemetry data.  Note that this mechanism does not strongly validate data at this time.  Instead, it builds a unique span key consisting equal to a non-exhaustive list of the flattened span dimensions (i.e., resource attrs, scope details/attrs, span name/attrs); it adds and subtracts from the map for each span expected and actually received, and will log errors when it encounters unexpected spans; when it finds unexpected spans, it prints the suspected match by scanning the list of expected spans for spans only matching name and SpanID fields.
