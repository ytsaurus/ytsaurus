# YTADMIN-13061 logslice fixtures

The three severity files retain a compact same-request excerpt from the
incident report. They exercise timestamp merging by the full request/cell IDs
without SSH or compressed logs.

`rotation_outcomes.json` records the three distinct selected-file outcomes the
wrapper previously collapsed: a matching rotation, grep exit 1 with no match,
and a real decompression failure. The regression requires a mixed
matched/no-match slice to succeed and an operational failure to remain fatal.

Replay with the Python logslice unit test target or:

```bash
python3 yt/yt/tools/logslice/unittests/py/test_logslice_py.py
```
