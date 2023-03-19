---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/watson.md
sourcePath: yql-product/udf/list/watson.md
---
# Watson UDF

Watson is designed to create parsers in just a couple of clicks. For more information.

This UDF lets you apply parsers to arbitrary HTML documents.

## Watson::FromConfig

Creates a callable parser based on the config.

Params:
* `String` *config* &mdash; a created Watson config with xpath selectors

Return value:
* `(Callable(String? html, String? url, Int64? charset) -> Struct of (Result:Yson?, Error:Yson?))` *parse* &mdash; a callable parser that accepts `html`, `url`, and `charset` and returns a structure containing the result: Result &mdash; Yson with parsing results, Error &mdash; Yson with an error.

Example:

```yql
USE arnold;
$parse = Watson::FromConfig(@@[
  {
    "xpath_selector": "descendant::title",
    "type": "field",
    "key": "title",
    "selector": "title",
    "sanitize": true
  }
]@@);
SELECT
    Url,
    $parse(Html, Url, Charset) as Result
FROM
    `home/search-functionality/geomslayer/WATSON-524/ivi_docs`
```
<!--[Operation](https://cluster-name.yql/Operations/XNq9ZJ9LnlvXLdwlPwmuf0HSxNsc0MauoXV3QISshCw=)-->

---

## Watson::FromConfigFile

Creates a callable parser based on the config passed in *File*. Then the parser is applied to HTML documents.

Params:
* `String` *configFile* &mdash; the path to the file with created Watson config with xpath selectors

Return value:
* `(Callable(String? html, String? url, Int64? charset) -> Struct<Yson? Result, Yson? Error>)` *parse* &mdash; a callable parser that accepts `html`, `url`, and `charset` and returns a structure containing the result: Result &mdash; Yson with parsing results, Error &mdash; Yson with an error.

Example:

```yql
PRAGMA File('ivi_config', 'https://proxy.sandbox.yandex-team.ru/949719847');
USE arnold;
$parse = Watson::FromConfigFile(FilePath('ivi_config'));
SELECT
    Url,
    $parse(Html, Url, Charset) as Result
FROM
    `home/search-functionality/geomslayer/WATSON-524/ivi_docs`
```
<!--[Operation](https://cluster-name.yql/Operations/XNq9s2im9QR8noPAVyuKamJVol8NriNH5P2OnAi3oxY=)-->

---

## Watson::FromParserList

Creates a callable parser from several parsers defined by structures.

Params:
* `List<Struct<String Id, String Host, String RegEx, String? Config, String? ConfigFile>>` *parsers* &mdash; a list of structures that define parsers.
   *Id* &mdash; parser's unique ID, *Host* &mdash; the host to be filtered by, *RegEx* &mdash; a regular expression for the URL to be filtered by.
   *Config* and *ConfigFile* are mutually exclusive.

Return value:
* `Callable(String? html, String? url, Int64? charset) -> List<Struct<String Id, Yson? Result, Yson? Error>>` *parse* &mdash; a callable parser that accepts `html`, `url`, and `charset` and returns a list of results in a structure: Id &mdash; passed through parser ID, Result &mdash; Yson with parsing results, Error &mdash; Yson with an error.

Example:

```yql
PRAGMA File('ivi_config', 'https://proxy.sandbox.yandex-team.ru/949719847');
USE arnold;
$parsers = AsList(
    AsStruct(
        'from_file' AS Id,
        'https://www.ivi.ru' AS Host,
        @@^https://www\.ivi\.ru/watch/[^/?#]+(/description)?[?#/]*$@@ AS RegEx,
        FilePath('ivi_config') AS ConfigFile
    ),
    AsStruct(
        'from_config' AS Id,
        'www.ivi.ru' AS Host,
        @@^https://(www\.)?ivi\.ru/watch/[^/?#]+(/description)?[?#/]*$@@ AS RegEx,
        @@[
          {
            "xpath_selector": "descendant::title",
            "type": "field",
            "key": "title",
            "selector": "title",
            "sanitize": true
          }
        ]@@ AS Config
    ),
);

$parse = Watson::FromParserList($parsers);
SELECT
    Url,
    $parse(Html, Url, Charset) as Result
FROM
    `home/search-functionality/geomslayer/WATSON-524/ivi_docs`
```
<!--[Operation](https://cluster-name.yql/Operations/XNq-a2Hljiffg8kx6dGjpiAr4gv33XrI-nTeZTHcnl0=)-->

---

## Watson::GetConfigUrl

Returns a link to the parser config using Parser ID (available on the parser page in the [admin](https://watson.yandex-team.ru) console). Is used together with `PRAGMA File`.

Example:

```yql
$ivi_config_url = Watson::GetConfigUrl('ivndex_www_ivi_ru_20180730_2014_52da8c');
PRAGMA File('ivi_config', $ivi_config_url);
USE arnold;
$parse = Watson::FromConfigFile(FilePath('ivi_config'));
SELECT
    Url,
    $parse(Html, Url, Charset) as Result
FROM
    `home/search-functionality/geomslayer/WATSON-524/ivi_docs`
```

You can specify a particular parser version in the second argument. To view all parser versions, open the History tab on the parser page in the [admin console](https://watson.yandex-team.ru).
If no parser version is specified, the latest version is used.

```yql
$ivi_config_url = Watson::GetConfigUrl('ivndex_www_ivi_ru_20180730_2014_52da8c', '2');
PRAGMA File('ivi_config', $ivi_config_url);
USE arnold;
$parse = Watson::FromConfigFile(FilePath('ivi_config'));
SELECT
    Url,
    $parse(Html, Url, Charset) as Result
FROM
    `home/search-functionality/geomslayer/WATSON-524/ivi_docs`
```
