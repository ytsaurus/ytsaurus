# SearchEngine UDF
``` yql
SearchEngine::Info(String{Flags:AutoMap}) -> Struct<
  IsAdvert: Bool,
  IsEmail: Bool,
  IsImage: Bool,
  IsLocal: Bool,
  IsSearch: Bool,
  IsSerpAdv: Bool,
  IsSocial: Bool,
  IsVideo: Bool,
  IsWeb: Bool,
  IsWebAdv: Bool,
  Name: String?,
  PageNum: String?,
  PageSize: String?,
  PageStart: String?,
  Platform: String?,
  Query: String?,
  Type: String?
>

SearchEngine::Is(String{Flags:AutoMap}) -> Bool
```

С помощью библиотеки [seinfo]({{source-root}}/kernel/seinfo) по переданному URL может определить, относится ли он к выдаче какой-либо из известных поисковых систем (`SearchEngine::Is`), а также подробности, которые можно из него извлечь (`SearchEngine::Info`). Как правило, подходящие для таких проверок URL получаются из HTTP referer переходов по ссылкам.

#### Примеры

``` yql
SELECT SearchEngine::Info(
  "https://yandex.ru/search/?offline_search=1&text=%D0%BF%D1%80%D0%BE%D0%B2%D0%B5%D1%80%D0%BA%D0%B0%20%D1%81%D0%B2%D1%8F%D0%B7%D0%B8&lr=213.4"
).Query; -- "проверка связи"

SELECT SearchEngine::Is("http://ya.ru"); --false
```
