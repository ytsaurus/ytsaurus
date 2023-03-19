---
vcsPath: Ыydb/docs/ru/core/yql/reference/yql-core/udf/list/pcre.md
sourcePath: Ыydb/docs/ru/core/yql/reference/yql-core/udf/list/pcre.md
---
# Pcre

The Pcre library is currently an alias to [Hyperscan](hyperscan.md).

If you rely on specific functionality of a certain regular expression engine, you should use a UDF with a particular library inside and treat Pcre as the current recommended option for simple matches, which might change in the future.

Currently available engines:

* [Hyperscan](hyperscan.md) <span style="color: gray;">(Intel)</span>
* [Pire](pire.md) <span style="color: gray;">(Yandex)</span>
* [Re2](re2.md) <span style="color: gray;">(Google)</span>

All three modules provide nearly the same functionality with an identical interface, which lets you switch between them while minimizing changes to your query.

Inside Hyperscan, there are several implementations that use different sets of processor instructions, with the relevant instruction automatically selected based on the current processor. In HyperScan, some functions support backtracking (referencing the previously found part of the string). Those functions are implemented through hybrid use of the two libraries: Hyperscan and libpcre.

[Pire](https://github.com/yandex/pire) (Perl Incompatible Regular Expressions) is a very fast regular expression library developed by Yandex. At the lower level, it parses the input string once without backtracking and uses 5 instructions per character (both for x86 and x86_64). However, its development was almost frozen in 2011–2013, so, as the name suggests ("i" stands for incompatible), you might have to adapt the expressions themselves.

Hyperscan and Pire are best-suited for Grep and Match.

Re2 uses [google::RE2](https://github.com/google/re2), which offers a wide range of options ([see official documentation](https://github.com/google/re2/wiki/Syntax)). The key advantage of Re2 is a broad Capture and Replace functionality. Use this library if you need these capabilities.
