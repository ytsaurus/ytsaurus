### Tool for dumping tablet cell changelogs in human-readable format.

How to get the changelog:

```
yt execute read_journal '{path="//sys/tablet_cells/x-x-x-x/changelogs/000012345";output_format=<format=binary>yson}' > journal
```

Presented version only dumps rows written with TReqWriteRows. Add your own mutation dumpers to mutation_dumper.h if you need them for other mutation types.
