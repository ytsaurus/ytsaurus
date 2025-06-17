# Dsv UDF

Functions that convert strings formatted as `"key1=value1\tkey2=value2"` to dictionaries.

```yql
Dsv::ReadRecord(Struct<key:String,subkey:String,value:String>) -> Struct<key:String,subkey:String,dict:Dict<String,String>>
Dsv::Parse(String,[String]) -> Dict<String,String> -- the second argument defines the delimiter (tab by default)
```

#### Example

```yql
SELECT Dsv::Parse("a=b@@c=d@@e=f", "@@")["c"]; -- "d"
```
