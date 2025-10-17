# sqllogictest_generator

Tool to generate unittests from sqlite sqllogic files.

Source files must have sqllogic test format described in the [sqlite documentation](https://sqlite.org/sqllogictest/doc/trunk/about.wiki).
Tool will generate YSON file for each provided source file. YSON file has following structure:

```
{
    "table_schema" = {
        "//t1" = "[{\"name\"=\"a\";\"type\"=\"int64\";};{\"name\"=\"b\";\"type\"=\"int64\";};{\"name\"=\"c\";\"type\"=\"int64\";};{\"name\"=\"d\";\"type\"=\"int64\";};{\"name\"=\"e\";\"type\"=\"int64\";};]";
    };
    "source_rows" = {
        "//t1" = [
            "\"a\"=104;\n\"b\"=100;\n\"c\"=102;\n\"d\"=101;\n\"e\"=103;\n";
        ];
    };
    "queries" = [
        {
            "name" = "query_4";
            "query" = "select a + b * 2 + c * 3 + d * 4, case a + 1 when b then 111 when c then 222 when d then 333 when e then 444 else 555 end, (select sum(1) from `//t1` as x where x.c > `//t1`.c and x.d < `//t1`.d), c from `//t1` where (c <= d - 2 or c >= d + 2) order by 4, 2, 1, 3 limit 1000000";
            "expected_rows" = [
                "<\"id\"=0;>1067;\n<\"id\"=1;>333;\n<\"id\"=2;>0;\n<\"id\"=3;>106;\n";
            ];
        };
    ];
}
```

Tool will generate cpp unittest from jinja template located here `yt/yt/tools/sqllogictest_generator/jinja/ql_sqllogic_ut.cpp.jinja`.
