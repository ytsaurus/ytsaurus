# Html UDF
Функции для работы с HTML.

``` yql
Html::Escape(String{Flags:AutoMap}) -> String
Html::EscapeAttributeValue(String{Flags:AutoMap}) -> String
Html::Strip(String{Flags:AutoMap}) -> String
```

#### Примеры

``` yql
SELECT Html::Escape("<html><h1>Hi!</h1></html>");   -- "&lt;html&gt;&lt;h1&gt;Hi!&lt;/h1&gt;&lt;/html&gt;"
SELECT Html::Strip("<html><h1>Hi!</h1></html>");    -- "Hi!"
```
