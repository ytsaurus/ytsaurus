# Mime UDF

Набор функций для работы с [принятым в Аркадии представлением Mime-Type]({{source-root}}/library/cpp/mime).

``` yql
Mime::Name(Uint32{Flags:AutoMap}) -> String
Mime::ContentType(Uint32{Flags:AutoMap}) -> String
```

#### Примеры

``` yql
SELECT Mime::Name(2);        -- "html"
SELECT Mime::ContentType(1); -- "text/plain"
```
