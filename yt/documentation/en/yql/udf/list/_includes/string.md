---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/udf/list/_includes/string.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/udf/list/_includes/string.md
---
# String
Functions for ASCII strings:

**List of functions**

* ```String::Base64Encode(string:String{Flags:AutoMap}) -> String```
* ```String::Base64Decode(string:String) -> String?```
* ```String::Base64StrictDecode(string:String) -> String?```
* ```String::EscapeC(string:String{Flags:AutoMap}) -> String```
* ```String::UnescapeC(string:String{Flags:AutoMap}) -> String```
* ```String::HexEncode(string:String{Flags:AutoMap}) -> String```
* ```String::HexDecode(string:String) -> String?```
* ```String::EncodeHtml(string:String{Flags:AutoMap}) -> String```
* ```String::DecodeHtml(string:String{Flags:AutoMap}) -> String```
* ```String::CgiEscape(string:String{Flags:AutoMap}) -> String```
* ```String::CgiUnescape(string:String{Flags:AutoMap}) -> String```

   Codes or decodes the string as specified.

**Example**

```sql
SELECT String::Base64Encode("YQL"); -- "WVFM"
```

* ```String::Strip(string:String{Flags:AutoMap}) -> String```

   Cuts the outermost white spaces off the string.

**Example**

```sql
SELECT String::Strip("YQL ");       -- "YQL"
```

* ```String::Collapse(string:String{Flags:AutoMap}) -> String```

   Replaces multiple white spaces within the string with singles.

* ```String::CollapseText(string:String{Flags:AutoMap}, limit:Uint64) -> String```

   Shortens text to a specified size by adding three dots.

* ```String::Contains(string:String?, substring:String) -> Bool```

   Checks the string for a substring.

* ```String::Find(string:String{Flags:AutoMap}, String, [Uint64?]) -> Int64``` — Outdated: use built-in `Find` function
* ```String::ReverseFind(string:String{Flags:AutoMap}, String, [Uint64?]) -> Int64``` — Outdated: use built-in `RFind` function
* ```String::Substring(string:String{Flags:AutoMap}, [Uint64?, Uint64?]) -> String``` — Outdated: use built-in `Substring`function
* ```String::HasPrefix(string:String?, prefix:String) -> Bool``` — Outdated: use built-in `StartsWith` function
* ```String::StartsWith(string:String?, prefix:String) -> Bool``` — Outdated: use built-in `StartsWith` function
* ```String::HasSuffix(string:String?, suffix:String) -> Bool``` — Outdated: use built-in `EndsWith` function
* ```String::EndsWith(string:String?, suffix:String) -> Bool``` — Outdated: use built-in `EndsWith` function
* ```String::Reverse(string:String?) -> String?``` - Outdated: use [Unicode::Reverse](../unicode.md)

   Using outdated functions is not recommended.

* ```String::HasPrefixIgnoreCase(string:String?, prefix:String) -> Bool```
* ```String::StartsWithIgnoreCase(string:String?, prefix:String) -> Bool```
* ```String::HasSuffixIgnoreCase(string:String?, suffix:String) -> Bool```
* ```String::EndsWithIgnoreCase(string:String?, suffix:String) -> Bool```

   Check the string for a prefix or suffix without considering character case.

* ```String::AsciiToLower(string:String{Flags:AutoMap}) -> String``` — replaces only Latin characters. If working with other alphabets, see Unicode::ToLower
* ```String::AsciiToUpper(string:String{Flags:AutoMap}) -> String``` — replaces only Latin characters. If working with other alphabets, see Unicode::ToUpper
* ```String::AsciiToTitle(string:String{Flags:AutoMap}) -> String``` — replaces only Latin characters. If working with other alphabets, see Unicode::ToTitle

   Reduces ASCII character case in the string to upper, lower, or title case.

* ```String::SplitToList(string:String?, delimeter:String, [ DelimeterString:Bool?, SkipEmpty:Bool?, Limit:Uint64? ]) -> List<String>```

   Splits the string into substrings with a separator.
   ```string``` -- source string
   ```delimeter``` -- separator
   Named parameters:
   - DelimeterString:Bool? — treating a delimiter as a string (true, by default) or a set of characters "any of" (false)
   - SkipEmpty:Bool? - whether to skip empty strings in the result, is false by default
   - Limit:Uint64? - Limits the number of fetched components (unlimited by default); if the limit is exceeded, the raw suffix of the source string is returned in the last item

**Example**

```sql
SELECT String::SplitToList("1,2,3,4,5,6,7", ",", 3 as Limit); -- ["1", "2", "3", "4,5,6,7"]
```

* ```String::JoinFromList(strings:List<String>{Flags:AutoMap}, separator:String) -> String```

   Concatenates the list of separator-delimited strings into a single string.

* ```String::ToByteList(string:String) -> List<Byte>```

   Splits the string into a list of bytes.

* ```String::FromByteList(bytes:List<Uint8>) -> String```

   Gathers the list of bytes into a string.

* ```String::ReplaceAll(input:String{Flags:AutoMap}, find:String, replacement:String) -> String```
* ```String::ReplaceFirst(input:String{Flags:AutoMap}, find:String, replacement:String) -> String```
* ```String::ReplaceLast(input:String{Flags:AutoMap}, find:String, replacement:String) -> String```

   Replaces all/first/last occurrence(s) of the ```find``` string in ```input``` with ```replacement```.

* ```String::RemoveAll(input:String{Flags:AutoMap}, symbols:String) -> String ```
* ```String::RemoveFirst(input:String{Flags:AutoMap}, symbols:String) -> String ```
* ```String::RemoveLast(input:String{Flags:AutoMap}, symbols:String) -> String ```

   Removes all/first/last occurrence(s) of a character in a set of ```symbols``` from ```input```. The second argument is interpreted as an unordered set of characters to be deleted.

* ```String::IsAscii(string:String{Flags:AutoMap}) -> Bool```

   Checks whether the string is a valid ASCII sequence.

* ```String::IsAsciiSpace(string:String{Flags:AutoMap}) -> Bool```
* ```String::IsAsciiUpper(string:String{Flags:AutoMap}) -> Bool```
* ```String::IsAsciiLower(string:String{Flags:AutoMap}) -> Bool```
* ```String::IsAsciiAlpha(string:String{Flags:AutoMap}) -> Bool```
* ```String::IsAsciiAlnum(string:String{Flags:AutoMap}) -> Bool```
* ```String::IsAsciiHex(string:String{Flags:AutoMap}) -> Bool```

   Checks whether the ASCII string satisfies the specified condition.

* ```String::LevensteinDistance(stringOne:String{Flags:AutoMap}, stringTwo:String{Flags:AutoMap}) -> Uint64```

   Calculates the Levenshtein distance for the passed strings.

* ```String::LeftPad(string:String{Flags:AutoMap}, size:Uint64, filler:[String?]) -> String```
* ```String::RightPad(string:String{Flags:AutoMap}, size:Uint64, filler:[String?]) -> String```

   Aligns text to a specified size by adding the specified character or white spaces.

* ```String::Hex(value:Uint64{Flags:AutoMap}) -> String```
* ```String::SHex(value:Int64{Flags:AutoMap}) -> String```
* ```String::Bin(value:Uint64{Flags:AutoMap}) -> String```
* ```String::SBin(value:Int64{Flags:AutoMap}) -> String```
* ```String::HexText(string:String{Flags:AutoMap}) -> String```
* ```String::BinText(string:String{Flags:AutoMap}) -> String```
* ```String::HumanReadableDuration(value:Uint64{Flags:AutoMap}) -> String```
* ```String::HumanReadableQuantity(value:Uint64{Flags:AutoMap}) -> String```
* ```String::HumanReadableBytes(value:Uint64{Flags:AutoMap}) -> String```
* ```String::Prec(Double{Flags:AutoMap}, digits:Uint64) -> String ```

   Prints out the value as specified.

{% note alert %}

The functions from the String library don't support Cyrillic and can only work with ASCII characters. Use functions from [Unicode](../unicode.md) to work with UTF-8-coded strings.

{% endnote %}
