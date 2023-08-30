# Hyperscan

[Hyperscan](https://www.hyperscan.io) is an open source library developed by Intel for searching with regular expressions.

The library features 4 implementations with different processor instruction sets (SSE3, SSE4.2, AVX2, and AVX512) that are chosen automatically depending on the current processor.

By default, all functions work in the single-byte mode. However, if the regular expression is a valid UTF-8 string but is not a valid ASCII string, the UTF-8 mode is enabled automatically.

**List of functions**

* ```Hyperscan::Grep(pattern:String) -> (string:String?) -> Bool```
* ```Hyperscan::Match(pattern:String) -> (string:String?) -> Bool```
* ```Hyperscan::BacktrackingGrep(pattern:String) -> (string:String?) -> Bool```
* ```Hyperscan::BacktrackingMatch(pattern:String) -> (string:String?) -> Bool```
* ```Hyperscan::MultiGrep(pattern:String) -> (string:String?) -> Tuple<Bool, Bool, ...>```
* ```Hyperscan::MultiMatch(pattern:String) -> (string:String?) -> Tuple<Bool, Bool, ...>```
* ```Hyperscan::Capture(pattern:String) -> (string:String?) -> String?```
* ```Hyperscan::Replace(pattern:String) -> (string:String?, replacement:String) -> String?```

## Call syntax {#syntax}

To avoid compiling a regular expression at each table row at direct call, wrap the function call by a [named expression](../../syntax/expressions.md#named-nodes).

```sql
$re = Hyperscan::Grep("\\d+");      -- we create a called value to test the specific regular expression
SELECT * FROM table WHERE $re(key); -- we use the value to filter the table
```

**Please note** escaping of special characters in regular expressions. Be sure to use the second slash, since all the standard string literals in SQL can accept C-escaped strings, and the `\d` sequence is not a valid sequence (even if it were, it wouldn't search for numbers as intended).


You can enable the case-insensitive mode by specifying, at the beginning of the regular expression, the flag `(?i)`.


## Grep {#grep}

Matches the regular expression with a **part of the string** (arbitrary substring).

## Match {#match}

Matches the **whole string** against the regular expression.


To get a result similar to that of `Grep` (that matches against a substring), enclose the regular expression in `.*` on both sides (`.*foo.*` instead of `foo`). However, in terms of code readability, it's usually better to change the function.

## BacktrackingGrep / BacktrackingMatch {#backtrackinggrep}

The functions are identical to the same-name functions without the `Backtracking` prefix. However, they support a broader range of regular expressions. This is due to the fact that if a specific regular expression is not fully supported by Hyperscan, the library switches to the prefilter mode. In this case, it responds not by "Yes" or "No", but by "Definitely not" or "Maybe yes". The "maybe yes" answers are then automatically rechecked using the slower but more powerful [libpcre](https://www.pcre.org) library.

## MultiGrep / MultiMatch {#multigrep}

Hyperscan lets you match against multiple regular expressions in a single pass through the text, and get a separate response for each match.


However, if you want to match a string against any of the listed expressions (the results would be joined with "or"), it would be more efficient to combine the query parts in a single regular expression with `|` and match it with regular `Grep` or `Match`.

When you call `MultiGrep`/`MultiMatch`, regular expressions are passed one per line using [multiline string literals](../../syntax/expressions.md#named-nodes):

**Example**

```sql
$multi_match = Hyperscan::MultiMatch(@@a.*
.*x.*
.*axa.*@@);

SELECT
    $multi_match("a") AS a,     -- (true, false, false)
    $multi_match("axa") AS axa; -- (true, true, true)
```

## Capture and Replace {#capture}

If a string matches the given regexp, `Hyperscan::Capture` returns the last substring matched by the regexp. `Hyperscan::Replace` replaces all substrings matched by the specified regexp with the given string.

Hyperscan offers limited capabilities for such operations. This means that although `Hyperscan::Capture` and `Hyperscan::Replace` are implemented for uniformity, we recommend using same-name functions from Re2 for non-obvious search and replacement operations:

* [Re2::Capture](re2.md#capture);
* [Re2::Replace](re2.md#replace).


## Usage example

```sql
$value = "xaaxaaXaa";

$match = Hyperscan::Match("a.*");
$grep = Hyperscan::Grep("axa");
$insensitive_grep = Hyperscan::Grep("(?i)axaa$");
$multi_match = Hyperscan::MultiMatch(@@a.*
.*a.*
.*a
.*axa.*@@);

$capture = Hyperscan::Capture(".*a{2}.*");
$capture_many = Hyperscan::Capture(".*x(a+).*");
$replace = Hyperscan::Replace("xa");

SELECT
    $match($value) AS match,                        -- false
    $grep($value) AS grep,                          -- true
    $insensitive_grep($value) AS insensitive_grep,  -- true
    $multi_match($value) AS multi_match,            -- (false, true, true, true)
    $multi_match($value).0 AS some_multi_match,     -- false
    $capture($value) AS capture,                    -- "xaa"
    $capture_many($value) AS capture_many,          -- "xa"
    $replace($value, "b") AS replace                -- "babaXaa"
;
```
