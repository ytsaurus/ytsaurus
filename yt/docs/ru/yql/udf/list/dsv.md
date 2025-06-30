# Dsv UDF

Функции для преобразования строк вида `"key1=value1\tkey2=value2"` в словари.

``` yql
Dsv::ReadRecord(Struct<key:String,subkey:String,value:String>) -> Struct<key:String,subkey:String,dict:Dict<String,String>>
Dsv::Parse(String,[String]) -> Dict<String,String> -- второй аргумент определяет разделитель, по умолчанию — табуляция
```

#### Пример

``` yql
SELECT Dsv::Parse("a=b@@c=d@@e=f", "@@")["c"]; -- "d"
```
