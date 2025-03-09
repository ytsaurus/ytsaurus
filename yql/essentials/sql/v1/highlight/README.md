# YQL Highlighting Specification

## Terms

- `Highlighting Unit` is a query text substring to be highlighted.

- `Token` is a token name from the [YQL Grammar][6].

- `Rule` is a production rule name from the [YQL Grammar][6].

- `YQL Grammar` is the YQL formal grammar represented in `ANTLR4` format, this is the single source of truth about the language syntax.

- `Theme` is a mapping from `Highlighting Unit` to a `Color`.

- `Color` is a `RGB`-equivalent `HEX`.

## Highlighting Unit

`Highlighting Unit` is detected using following rule. Every rule is a list of token sequences (maybe with the length 1). The elements are meant to be combined using an OR regular expression operator. Implicitly there are indefinite number `WS`, `COMMENT` between each token in a rule. For complex rules only sequences inside braces should be highlighted, so other tokens serve as a hint.

- `Keyword` - TODO(vityaman): list keywords automatically from the grammar.

- `Punctuation` - `(GREATER GREATER)`, `(GREATER GREATER PIPE)`, `(QUESTION QUESTION)`, TODO(vityaman): list punctuation automatically from the grammar.

- `Identifier` - `ID_PLAIN`.

- `Quoted Identifier` - `ID_QUOTED`.

- `Bind Parameter Identifier` - `DOLLAR ID_PLAIN`.

- `Type Identifier` - `(ID_PLAIN) LPAREN`, `Decimal`, `Bool`, `Int8`, `Int16`, `Int32`, `Int64`, `Uint8`, `Uint16`, `Uint32`, `Uint64`, `Float`, `Double`, `Decimal`, `DyNumber`, `String`, `Utf8`, `Json`, `JsonDocument`, `Yson`, `Uuid`, `Date`, `Datetime`, `Timestamp`, `Interval`, `TzDate`, `TzDateTime`, `TzTimestamp`, `Callable`, `Resource`, `Tagged`, `Generic`, `Unit`, `Null`, `Void`, `EmptyList`, `EmptyDict`.

TODO(vityaman): list primitive and special types automatically from docs.

- `Function Identifier` - `(ID_PLAIN NAMESPACE ID_PLAIN)`, `(ID_PLAIN) LPAREN`.

- `Boolean Literal` - `"True"`, `"False"`.

- `Number Literal` - `DIGITS`, `INTEGER_VALUE`, `REAL`, `BLOB`.

- `String Literal` - `STRING_VALUE`.

- `Object Feature Value Literal` - `ENABLED`, `DISABLED`.

- `Comment` - `COMMENT`.

## Token Regex

TODO(vityaman): list token regexes automatically from the YQL grammar.

## Themes

| Highlighting Unit                | Monaco Light | Monaco Dark |
|----------------------------------|--------------|-------------|
| **Keyword**                      | #0000ff      | #569cd6     |
| **Punctuation**                  | #000000      | #d4d4d4     |
| **Identifier**                   | #001188      | #74b0df     |
| **Quoted Identifier**            | #338186      | #338186     |
| **Bind Parameter Identifier**    | TODO         | TODO        |
| **Type Identifier**              | #4d932d      | #6A8759     |
| **Function Identifier**          | #7a3e9d      | #9e7bb0     |
| **Boolean Literal**              | TODO         | TODO        |
| **Number Literal**               | #608b4e      | #608b4e     |
| **String Literal**               | #a31515      | #ce9178     |
| **Object Feature Value Literal** | TODO         | TODO        |
| **Comment**                      | #969896      | #969896     |

TODO(vityaman): for a better reusability instead of concrete colors - links to the original theme aliases. This is not a specification of color themes, but a mapping from YQL constructs into theme color codes.

## Implementation Guidelines

- The module `yql/essentials/sql/v1/highlight` is a reference implementation of the YQL highlighting. Module includes a comprehensive test suite to check an implementation compliance with the specification. Also this module contains this specification document.

## Implementation Examples

- YQL service UI (Web, ???)
- [Yandex Query (Web, ???)][1]
- YDB UI (Web, ???)
- [YDB Embedded UI (Web, TypeScript, Prism)][2]
- [YDB CLI (Terminal, C++, ANTLR4 ad-hoc)][3]

TODO(vityaman): coordinates of unknown services including their implementation details.

## Design Decisions

- Список плоский, не вложенный, потому что непонятно, как выражать иерархию в реализации (поддержка языком программирования). Имена цветом уточняются слева прилагательными (fluent), чтобы обеспечить плоский список. Плоскость списка валидна, так как фактические цвета едва ли будут связаны вложенностью (не совсем правда). Но все-таки он не плоский.

TODO(vityaman): сформулировать пункт выше нормально, сейчас какая-то дичь.

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
