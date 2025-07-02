# Функции встроенных C++ библиотек

Многие прикладные функции, которые с одной стороны слишком специфичны, чтобы стать частью ядра YQL, а с другой — могут быть полезны широкому кругу пользователей, доступны через встроенные C++ библиотеки.

Для подключения встроенных C++ библиотек используется механизм С++ UDF.

## Список функций

* [Compress Decompress](compress)
* [DateTime](datetime.md)
* [Digest](digest.md)
{% if audience == "internal" %}
* [H3](h3.md)
{% endif %}
* [Histogram](histogram.md)
{% if audience == "internal" %}
* [HTML](html.md)
{% endif %}
* [Hyperscan](hyperscan.md)
* [Ip](ip.md)
{% if audience == "internal" %}
* [Geo](geo.md)
* [GeoBase](geobase.md)
* [GeoHash](geohash.md)
{% endif %}
* [Math](math.md)
{% if audience == "internal" %}
* [Mime](mime.md)
{% endif %}
* [Pcre](pcre.md)
* [Pire](pire.md)
* [Protobuf](protobuf.md)
* [Re2](re2.md)
{% if audience == "internal" %}
* [SearchEngine](searchengine.md)
{% endif %}
* [String](string.md)
* [Unicode](unicode.md)
* [Url](url.md)
{% if audience == "internal" %}
* [UserAgent](useragent)
* [XML](xml.md)
{% endif %}
* [Yson](yson.md)

Будут добавлены в ближайшее время:
* ClickHouse
* PostgreSQL

{% if audience == "internal" %}
## Проектные UDF

Среди встроенных C++ библиотек есть также проектные UDF от сторонних команд. Эти функции не описаны в текущей документации, но вы можете использовать их в своих запросах. Описания к ним читайте в README [директории]({{source-root}}/yql/udfs) с функцией или на стенде [документации YQL]({{yql.docs}}/udf/list). Однако следует иметь в виду, что некоторые проектные UDF могут не работать в [QueryTracker]({{production-cluster-qt}}), даже если они доступны на [сервисе YQL]({{yql.link}}).
{% endif %}
