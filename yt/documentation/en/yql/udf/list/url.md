---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/url.md
sourcePath: yql-product/udf/list/url.md
---

{% include [url.md](_includes/url.md) %}

## Yandex extensions

For more information about Punycode, see [comments in the utilized library](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/unicode/punycode/punycode.h).

## IsAllowedByRobotsTxt
```yql
Url::IsAllowedByRobotsTxt(
  String{Flag:AutoMap}, -- URL
  String?, -- binary representation of robots.txt in the format of https://a.yandex-team.ru/arc/trunk/arcadia/library/robots_txt
  Uint32 -- robot ID from https://a.yandex-team.ru/arc/trunk/arcadia/library/robots_txt/robotstxtcfg.h?rev=3514373#L14
) -> Bool
```
