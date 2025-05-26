# Protobuf UDF
Преобразование protobuf-сообщений в структуры YQL.

Список функций:

`Protobuf::Parse(String?{Flags:AutoMap}) -> <Struct>?` &mdash; распаковка сообщения в YQL структуру.
`Protobuf::TryParse(String?{Flags:AutoMap}) -> <Struct>?` &mdash; безопасная распаковка сообщения, в случае ошибки возвращает Null.
`Protobuf::Serialize(<Struct>?{Flags:AutoMap}) -> String` &mdash; сериализация структуры в protobuf-строку.

Перед использованием необходимо создать метаинформацию, аналогичную той, что записывается в мета-атрибут `_yql_proto_field`<!--[_yql_proto_field](../../misc/schema.md#_yql_proto_field)-->.

## Поддерживаемые поля дескриптора {#yql_proto_field_desc}
  * `name` &mdash; имя protobuf-сообщения. Представляет собой конкатенацию имени пакета и имени сообщения, разделённых точкой &mdash; "package.message", где:
    * имя package &mdash; в рамках которого лежит proto файл;
    * имя message &mdash; в рамках которого собираются данные.
  * `meta` &mdash; дескриптор protobuf-сообщения (сжатый zlib, base64).
  * `format` &mdash; формат сериализации protobuf. Возможные значения: `protobin` (по умолчанию) &mdash; бинарный, `prototext` &mdash; текстовый, `json`.
  * `skip` &mdash; количество байт, которое игнорируется в каждом значении перед парсингом (по умолчанию 0).
  * `view` &mdash; словарь, определяющий настройки представления protobuf-сообщений. Поддерживаются следующие настройки:
    * `enum` &mdash; представление перечислений protobuf в YQL. Возможные значения: `number` (по умолчанию), `name`, `full_name`.
    * `recursion` &mdash; способ обработки рекурсивных сообщений. Возможные значения: `fail` (по умолчанию) &mdash; выдавать ошибку, `ignore` &mdash; пропускать рекурсивные вставки, `bytes` (deprecated) &mdash; преобразовывать рекурсивные вставки в байтовые строки, `bytesV2` &mdash; то же самое, что и bytes, но с исправлением выходного типа на опциональные байтовые строки для optional полей.
    * `yt_mode` &mdash; `YT Mode` (по умолчанию `false`), в котором обрабатываются YT-специфичные опции protobuf. Если в сообщении не указана опция `NYT.default_field_flags` или её значение не равно `SERIALIZATION_YT`, то сообщение будет преобразовано в байтовую строку. В случае `SERIALIZATION_YT` сообщение будет обработано обычным образом (при этом обработка рекурсивных сообщений определяется опцией `recursion`).
  * `lists`— словарь, определяющий настройки представления repeated полей protobuf-сообщений. Поддерживаются следующие настройки:
    * `optional` &mdash; представлять repeated поля как опциональные списки вместо обычных (по умолчанию `true`).

{% note warning "Внимание" %}

При использовании настройки `lists` `optional=false` не рекомендуется результат `Protobuf::Parse` записывать в таблицы, т.&nbsp;к. при появлении новых repeated полей схема таблицы станет несовместимой.

{% endnote %}

## Примеры

{% if audience == "internal" %}

```yql
USE Hahn;

$config = @@{
    "name": "ru.yandex.metrika.wv2.proto.BufferWrapper",
    "meta": "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"
}@@;

$try_parse = Udf(Protobuf::TryParse, $config As TypeConfig);

SELECT $try_parse(data) AS proto, $try_parse('BAD ROW') AS bad_row
FROM `//home/yql/protobuf_example`;
```

- [Пример в веб-интерфейсе Parse]({{yql.link}}/Operations/W64e3Da9vIEIfHUFBPytZeXuud362mbq8ewxUm4HJqU=)
- [Пример в веб-интерфейсе TryParse]({{yql.link}}/Operations/W8uTomUObwCuZFY_91u2i9L64dpCg9Ji73HwTeNIOfs=)

{% else %}

```yql
$config = @@{
    "name": "my.service.proto.BufferWrapper",
    "meta": "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"
}@@;

$try_parse = Udf(Protobuf::TryParse, $config As TypeConfig);

SELECT $try_parse(data) AS proto, $try_parse('BAD ROW') AS bad_row
FROM `//home/yql/protobuf_example`;
```
{% endif %}
