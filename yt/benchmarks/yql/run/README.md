# TPC-DS queries for YQL

Query parameters come from TPC-DS 4.0.0 `dsqgen`, using scale factor 3000,
seed 19620718, and one stream without `-QUALIFY`. The substitutions are recorded
in [`parameters/sf3000-seed19620718.txt`](parameters/sf3000-seed19620718.txt).

To reproduce them, build `dsqgen` and `tpcds.idx`, copy
[`parameters/dsqgen.tpl`](parameters/dsqgen.tpl) to the suite's `query_templates/`,
and run from its `tools/` directory:

```sh
mkdir -p /tmp/tpcds-queries
./dsqgen -INPUT ../query_templates/templates.lst \
    -DIRECTORY ../query_templates -DIALECT dsqgen \
    -SCALE 3000 -RNGSEED 19620718 -STREAMS 1 \
    -OUTPUT_DIR /tmp/tpcds-queries -LOG /tmp/tpcds-queries/parameters.txt
```

Generate all templates together: generation order affects the parameters.
Preserve the existing YQL adaptations when applying the generated substitutions.
