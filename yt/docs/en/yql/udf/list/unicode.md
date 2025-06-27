# Unicode

Functions for Unicode strings.

## List of functions

* `Unicode::IsUtf(String) -> Bool`

  Checks whether a string is a valid utf-8 sequence. For example, the string `"\xF0"` isn't a valid utf-8 sequence, while the string `"\xF0\x9F\x90\xB1"` correctly describes a utf-8 cat emoji.

* `Unicode::GetLength(Utf8{Flags:AutoMap}) -> Uint64`

  Returns the utf-8 string's length in characters (unicode code points). Surrogate pairs are counted as one character.

  ```yql
  SELECT Unicode::GetLength("жніўня"); -- 6
  ```

* `Unicode::Find(string:Utf8{Flags:AutoMap}, subString:Utf8, [pos:Uint64?]) -> Uint64?`
* `Unicode::RFind(string:Utf8{Flags:AutoMap}, subString:Utf8, [pos:Uint64?]) -> Uint64?`

  Searches for the first (or the last in the case of `RFind`) substring occurrence in the string starting from the `pos` position. Returns the first character position from the found string. Null is returned in case of failure.

  ```yql
  SELECT Unicode::Find("aaa", "bb"); -- Null
  ```

* `Unicode::Substring(string:Utf8{Flags:AutoMap}, from:Uint64?, len:Uint64?) -> Utf8`

  Returns a substring from the `string`, starting from the `from` character, with length equaling `len` characters. If the `len` argument is omitted, the substring is taken to the end of the source string. If `from` is greater than the source string length, an empty `""` string is returned.

  ```yql
  SELECT Unicode::Substring("0123456789abcdefghij", 10); -- "abcdefghij"
  ```

* `Unicode::Normalize...` functions reduce the transmitted utf-8 string to a [normal form](https://unicode.org/reports/tr15/#Norm_Forms):

  * `Unicode::Normalize(Utf8{Flags:AutoMap}) -> Utf8` -- NFC
  * `Unicode::NormalizeNFD(Utf8{Flags:AutoMap}) -> Utf8`
  * `Unicode::NormalizeNFC(Utf8{Flags:AutoMap}) -> Utf8`
  * `Unicode::NormalizeNFKD(Utf8{Flags:AutoMap}) -> Utf8`
  * `Unicode::NormalizeNFKC(Utf8{Flags:AutoMap}) -> Utf8`


* `Unicode::Translit(string:Utf8{Flags:AutoMap}, [lang:String?]) -> Utf8`

  Transliterates with Latin letters the words from the passed string, consisting entirely of characters of the alphabet of the language passed by the second argument. If no language is specified, the words are transliterated from Russian. Available languages: "kaz", "rus", "tur", and "ukr".

  ```yql
  SELECT Unicode::Translit("Тот уголок земли, где я провел"); -- "Tot ugolok zemli, gde ya provel"
  ```

* `Unicode::LevensteinDistance(stringA:Utf8{Flags:AutoMap}, stringB:Utf8{Flags:AutoMap}) -> Uint64`

  Calculates the Levenshtein distance for the passed strings.

* `Unicode::Fold(Utf8{Flags:AutoMap}, [ Language:String?, DoLowerCase:Bool?, DoRenyxa:Bool?, DoSimpleCyr:Bool?, FillOffset:Bool? ]) -> Utf8`

  Performs [case folding](https://www.w3.org/TR/charmod-norm/#definitionCaseFolding) for the transmitted string.
  Parameters:

  - `Language` is set according to the same rules as in `Unicode::Translit()`.
  - `DoLowerCase` converts a string to lowercase letters, `true` by default.
  - `DoRenyxa` converts diacritical characters to similar Latin characters, `true` by default.
  - `DoSimpleCyr` converts diacritical Cyrillic characters to similar Latin characters, `true` by default.
  - `FillOffset` parameter is not used.

  ```yql
  SELECT Unicode::Fold("Kongreßstraße",  false AS DoSimpleCyr, false AS DoRenyxa); -- "kongressstrasse"
  SELECT Unicode::Fold("ҫурт"); -- "сурт"
  SELECT Unicode::Fold("Eylül", "Turkish" AS Language); -- "eylul"
  ```

* `Unicode::ReplaceAll(input:Utf8{Flags:AutoMap}, find:Utf8, replacement:Utf8) -> Utf8`
* `Unicode::ReplaceFirst(input:Utf8{Flags:AutoMap}, find:Utf8, replacement:Utf8) -> Utf8`
* `Unicode::ReplaceLast(input:Utf8{Flags:AutoMap}, find:Utf8, replacement:Utf8) -> Utf8`

  Replaces all/first/last occurrence(s) of the `find` string in `input` through `replacement`.

* `Unicode::RemoveAll(input:Utf8{Flags:AutoMap}, symbols:Utf8) -> Utf8`
* `Unicode::RemoveFirst(input:Utf8{Flags:AutoMap}, symbols:Utf8) -> Utf8`
* `Unicode::RemoveLast(input:Utf8{Flags:AutoMap}, symbols:Utf8) -> Utf8`

  Removes all/first/last occurrence(s) of characters defined in a `symbols` set from `input`. The second argument is interpreted as an unordered set of characters to be deleted.

  ```yql
  SELECT Unicode::ReplaceLast("absence", "enc", ""); -- "abse"
  SELECT Unicode::RemoveAll("abandon", "an"); -- "bdo"
  ```

* `Unicode::ToCodePointList(Utf8{Flags:AutoMap}) -> List<Uint32>`

  Splits the string into a unicode sequence of codepoints.
* `Unicode::FromCodePointList(List<Uint32>{Flags:AutoMap}) -> Utf8`

  Forms a unicode string from codepoints.

  ```yql
  SELECT Unicode::ToCodePointList("Щавель"); -- [1065, 1072, 1074, 1077, 1083, 1100]
  SELECT Unicode::FromCodePointList(AsList(99,111,100,101,32,112,111,105,110,116,115,32,99,111,110,118,101,114,116,101,114)); -- "code points converter"
  ```

* `Unicode::Reverse(Utf8{Flags:AutoMap}) -> Utf8`

  Reverses the string.

* `Unicode::ToLower(Utf8{Flags:AutoMap}) -> Utf8`
* `Unicode::ToUpper(Utf8{Flags:AutoMap}) -> Utf8`
* `Unicode::ToTitle(Utf8{Flags:AutoMap}) -> Utf8`

  Changes the character case in the string to upper, lower, or title case.

* `Unicode::SplitToList( string:Utf8?, separator:Utf8, [ DelimeterString:Bool?,  SkipEmpty:Bool?, Limit:Uint64? ]) -> List<Utf8>`

  Splits the string into substrings with a separator.
  `string` — Source string.
  `separator` — Separator.
  Parameters:

  - DelimeterString:Bool? — treating a separator as a string (`true` by default) or a set of characters "any of" (`false`).
  - SkipEmpty:Bool? — whether to skip empty strings in the result, `false` by default.
  - Limit:Uint64? — limits the number of fetched components (unlimited by default); if the limit is exceeded, the raw suffix of the source string is returned in the last item.

* `Unicode::JoinFromList(List<Utf8>{Flags:AutoMap}, separator:Utf8) -> Utf8`

  Concatenates a list of strings into a single string using a `separator`.

  ```yql
  SELECT Unicode::SplitToList("One, two, three, four, five", ", ", 2 AS Limit); -- ["One", "two", "three, four, five"]
  SELECT Unicode::JoinFromList(["One", "two", "three", "four", "five"], ";"); -- "One;two;three;four;five"
  ```

* `Unicode::ToUint64(string:Utf8{Flags:AutoMap}, [prefix:Uint16?]) -> Uint64`

  Converts from the string into a number. The second optional argument sets the numeral system. By default, 0 (automatic detection by prefix).

  Supported prefixes: `0x(0X)` (base-16), `0` (base-8). Default system: base-10.
  The `-` sign before a number is interpreted as in the C's unsigned arithmetic. For example, `-0x1` -> UI64_MAX.
  If a string contains invalid characters, or a number is beyond ui64, the function finishes with an error.

* `Unicode::TryToUint64(string:Utf8{Flags:AutoMap}, [prefix:Uint16?]) -> Uint64?`

  Similar to the `Unicode::ToUint64()` function, but returns `NULL` instead of an error.

  ```yql
  SELECT Unicode::ToUint64("77741"); -- 77741
  SELECT Unicode::ToUint64("-77741"); -- 18446744073709473875
  SELECT Unicode::TryToUint64("asdh831"); -- Null
  ```

* `Unicode::Strip(string:Utf8{Flags:AutoMap}) -> Utf8`

  Cuts off the string's outermost characters of Space Unicode category.

  ```yql
  SELECT Unicode::Strip("\u2002ыкль\u2002"u); -- "ыкль"
  ```

* `Unicode::IsAscii(string:Utf8{Flags:AutoMap}) -> Bool`

  Checks whether the utf-8 string consists exclusively of ASCII characters.

* `Unicode::IsSpace(string:Utf8{Flags:AutoMap}) -> Bool`
* `Unicode::IsUpper(string:Utf8{Flags:AutoMap}) -> Bool`
* `Unicode::IsLower(string:Utf8{Flags:AutoMap}) -> Bool`
* `Unicode::IsAlpha(string:Utf8{Flags:AutoMap}) -> Bool`
* `Unicode::IsAlnum(string:Utf8{Flags:AutoMap}) -> Bool`
* `Unicode::IsHex(string:Utf8{Flags:AutoMap}) -> Bool`

  Checks whether the utf-8 string satisfies the specified condition.

* `Unicode::IsUnicodeSet(string:Utf8{Flags:AutoMap}, unicode_set:Utf8) -> Bool`

  Checks whether the utf-8 `string` consists exclusively of characters indicated in `unicode_set`. Characters in `unicode_set` must be indicated in square brackets.

  ```yql
  SELECT Unicode::IsUnicodeSet("ваоао"u, "[вао]"u); -- true
  SELECT Unicode::IsUnicodeSet("ваоао"u, "[ваб]"u); -- false
  ```

{% if audience == "internal" %}
The Unicode library is based on Arcadia's `util/charset/` and `library/cpp/unicode/`.
{% endif %}
