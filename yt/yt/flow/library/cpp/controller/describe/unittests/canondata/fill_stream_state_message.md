#### Streams in topological order
| Stream ID | Computation ID | State | Example active partition |
|-----------|----------------|-------|--------------------------|
| Source | Computation_3 | Drained | <null> |
| timer_stream | Computation_1 | Completed | <null> |
| timer_stream | Computation_2 | Completed | <null> |
| output_stream | Computation_1 | Active | <hide Computation_1 partition> |
| output_stream | Computation_2 | Active | <null> |
| output_stream | Computation_3 | Active | <null> |
