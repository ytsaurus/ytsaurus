---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/url.md
sourcePath: yql-product/udf/list/url.md
---

{% include [url.md](_includes/url.md) %}

## Дополнения Yandex

Подробнее про Punycode смотрите [комментарии в используемой библиотеке](https://a.yandex-team.ru/arc/trunk/arcadia/library/cpp/unicode/punycode/punycode.h). ???

## IsAllowedByRobotsTxt
``` yql
Url::IsAllowedByRobotsTxt(
  String{Flag:AutoMap}, -- URL
  String?, -- бинарное представление robots.txt в формате https://a.yandex-team.ru/arc/trunk/arcadia/library/robots_txt ???
  Uint32 -- идентификатор робота из https://a.yandex-team.ru/arc/trunk/arcadia/library/robots_txt/robotstxtcfg.h?rev=3514373#L14 ???
) -> Bool
```