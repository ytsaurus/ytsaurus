---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/searchrequest.md
sourcePath: yql-product/udf/list/searchrequest.md
---
# SearchRequest UDF

Working with search queries using [ysite/yandex/reqanalysis](https://a.yandex-team.ru/arc/trunk/arcadia/ysite/yandex/reqanalysis).

Maintainers:

* <a href="https://staff.yandex-team.ru/gotmanov">Alexander Gotmanov</a>
* <a href="https://staff.yandex-team.ru/ilnurkh">Ilnur Khiziev</a>

```yql
SearchRequest::NormalizeFast(Utf8{Flags:AutoMap}) -> Utf8
SearchRequest::NormalizeSimple(Utf8{Flags:AutoMap}) -> Utf8 -- uses the same normalization
-- that's used in `relev:norm`
SearchRequest::NormalizeAttribute(Utf8{Flags:AutoMap}) -> Utf8 -- removes attributes from the query
SearchRequest::NormalizeBert(Utf8{Flags:AutoMap}) -> Utf8 -- normalization for BERT (plus Unicode NFC normalization)
SearchRequest::NormalizeBertNFD(Utf8{Flags:AutoMap}) -> Utf8 -- normalization for BERT (plus Unicode NFD normalization)
SearchRequest::NormalizeConsistent(Utf8{Flags:AutoMap}) -> Utf8 -- normalization for search queries
SearchRequest::NormalizeDopp(Utf8{Flags:AutoMap}, List<String>?, List<String>?) -> Utf8 -- doppelgangers. The main and auxiliary language lists can be specified. If no language is specified, Rus + Eng are used as the main languages and Ukr as the auxiliary.
SearchRequest::NormalizeSynnorm(Utf8{Flags:AutoMap}, List<String>?) -> Utf8 -- doppelgangers. Used languages can be specified. If no language is specified, Rus + Eng are used
-- identifying the query language
--1st argument is the query
-- 2nd argument is regionId
-- 3rd argument is the domain
-- 4th argument is the interface language
-- 5th argument is boosted_language
SearchRequest::RecognizeMainLang(Utf8{Flags:AutoMap}, Int32, String?, String?, String?) -> List<String>? -- contains one element or NULL
SearchRequest::RecognizeMaxCoveringLang(Utf8{Flags:AutoMap}, Int32, String?, String?, String?) -> List<String>? -- contains one element or NULL
SearchRequest::RecognizeAllLangs(Utf8{Flags:AutoMap}, Int32, String?, String?, String?) -> List<String>?
SearchRequest::RecognizeBestLangs(Utf8{Flags:AutoMap}, Int32, String?, String?, String?) -> List<String>?
-- for secondary goals and to check how string representation of the language is defined in functions, we added
SearchRequest::ParseLangList(List<String>{Flags:AutoMap})->List<String> -- tries to parse input strings as language markers, and returns the list of identified languages
```

SearchRequest::NormalizeSimple — a relatively slow function that:

1. removes special characters (punctuation marks)
2. removes Yandex query operators, including attributes like "site:blabla.com"
3. uses language-specific lowercase
4. multitokens (alphanumerical strings, such as "i7" or "т34") can be kept or split into components

**Examples**:

```yql
SELECT SearchRequest::NormalizeFast(
  CAST("Карамба</span>!" AS Utf8)
); -- "карамба span"
SELECT SearchRequest::NormalizeSimple(
  CAST('!fallout 2 ! in a row' AS Utf8)
); -- "fallout 2 in a row"
```

<!--[Example: detecting the string language](https://cluster-name.yql/Operations/X5sefJ3udoH5ywvrHLpa7W_wktfbRI4EruYpxnkEQO0=)-->
