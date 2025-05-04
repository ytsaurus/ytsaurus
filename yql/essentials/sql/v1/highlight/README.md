# YQL SQL Syntax Highlighting Specfication

## Terms

- `Highlighting` is a _list_ of `Highlighting Unit`s.

- `Highlighting Unit` is a language construction in a text to be highlighted.

- `Highlighting Token` is a text fragment matched with a `Highlighting Unit`.

- `Highlighter` is an function parametrized by `Highlighting` transforming a text into a stream of `Highlighting Token`.

- `Theme` is a mapping from `Highlighting Unit` to a `Color`.

- `Color` is a `RGB`-equivalent `HEX`.

## Highlighting Unit

Here are listed all highlighting units.

- Examples of a `keyword` includes `SELECT`, `INSERT`.

- Examples of a `punctuation` includes `.`, `;`, `(`.

- Examples of a `identifier` includes `example`.

- Examples of a `quoted-identifier` includes ``` `/local/example` ```.

- Examples of a `bind-parameter-identifier` includes `$subquery`.

- Examples of a `type-identifier` includes `UInt64`, `Optional<...>`.

- Examples of a `function-identifier` includes `StartsWith(...)`, `DateTime::Split`.

- Examples of a `literal` includes `1231u`, `1E+20`.

- An example of a `string-literal` is `"relax"`.

- Examples of a `comment` includes `-- scale it easy`, `/* touch the grass */`.

- Examples of a `ws` includes `' '`, `'\n'`.

- Examples of a `error` includes `!`.

`Highlighting Unit` has list of `Pattern`s. Each `Pattern` has a `body` and `after` (lookahead) properties that define an equivalent to regexp `body(?=after)`. Also it has the property `is-case-insensitive`.

## Highlighter Algorithm

The string `S` is matched by a `Highlighting Unit` `U` iff any of `U`s patterns matched the `S`.

To highlight a query prefix `P` the `Highlighter` iterates over `Highlighting Unit`s patterns in the specified order and collect matched. Then it choices the max by length matched pattern `P` corresponding to the `Highlighting Unit` `U`. If there are multiple patterns matched, leftmost in the `Highlighting Unit`s list is chosen.

The `Highlighter` produces the `Highlighting Token` with the kind of `U` and advances the text cursor by the length of the content matched by `P`.

## Matching ANSI Comments

Matching ANSI comments using regex is impossible as they are recursive. Therefore client should implement an algorithm to match an ANSI comment by hand. The algorithm `MatchANSI` takes a string as an input and produces an prefix of a matched comment.

1. Consume the begining of multiline comment `/*`.

2. If the current input starts with the miltiline comment end, then consume it and return the whole consumed prefix.

3. If the current input starts with the begining of multiline comment `/*`, then find the first ending of multiline comment `*/` from the end of the current input. We need to "book" it because ANSI comment must be closed.

    1. If it is not found, then `MatchANSI` failed.

    2. Else `MatchANSI` on a current input prefix until the first ending of multiline comment `*/` from the end of the current input.

4. If recursive `MatchANSI` succeded, then repeat from the step 2.

5. Else if input is ended, then `MatchANSI` failed.

6. Consume a one symbol and repeat from the step 2.

## Implementation Guidelines

- The module `yql/essentials/sql/v1/highlight` is a reference implementation of the `YQL` highlighting. Module includes a comprehensive test suite to check an implementation compliance with the specification. Also this module contains this specification document.

- The module `yql/essentials/tools/yql_highlight` contains a tool to play with the reference highlighting implementation and to generate various representation of highlighting (e.g. in JSON).

## Reference

- [Yandex Query UI URL][1]
- [YDB Embedded UI, YqlHighlighter][2]
- [YDB CLI, YQLHighlight][3]
- [YQL Syntax Reference][4]
- [VSCode Default Themes][5]
- [YQL ANTLR4 Grammar][6]

[1]: https://yq.yandex.cloud
[2]: https://github.com/ydb-platform/ydb-embedded-ui/tree/v8.12.0/src/components/YqlHighlighter
[3]: https://github.com/ydb-platform/ydb/blob/CLI_2.19.0/ydb/public/lib/ydb_cli/commands/interactive/yql_highlight.h
[4]: https://ydb.tech/docs/en/yql/reference/syntax/
[5]: https://github.com/microsoft/vscode/tree/fb28dca732cce37e797b83814e694bb14ba04b9a/extensions/theme-defaults/themes
[6]: https://github.com/ydb-platform/ydb/blob/CLI_2.19.0/yql/essentials/sql/v1/SQLv1Antlr4.g.in
