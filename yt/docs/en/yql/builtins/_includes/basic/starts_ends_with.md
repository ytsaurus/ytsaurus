
## StartsWith, EndsWith {#starts_ends_with}

Checking for a prefix or suffix in a string.

**Signatures**
```
StartsWith(Utf8, Utf8)->Bool
StartsWith(Utf8[?], Utf8[?])->Bool?
StartsWith(String, String)->Bool
StartsWith(String[?], String[?])->Bool?

EndsWith(Utf8, Utf8)->Bool
EndsWith(Utf8[?], Utf8[?])->Bool?
EndsWith(String, String)->Bool
EndsWith(String[?], String[?])->Bool?
```

Mandatory arguments:

* Source string;
* The substring being searched for.

The arguments can be of the `String` or `Utf8` type and can be optional.

**Examples**
```yql
SELECT StartsWith("abc_efg", "abc") AND EndsWith("abc_efg", "efg"); -- true
```
```yql
SELECT StartsWith("abc_efg", "efg") OR EndsWith("abc_efg", "abc"); -- false
```
```yql
SELECT StartsWith("abcd", NULL); -- null
```
```yql
SELECT EndsWith(NULL, Utf8("")); -- null
```
