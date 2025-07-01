# Функции встроенных C++ библиотек

Многие прикладные функции, которые с одной стороны слишком специфичны, чтобы стать частью ядра YQL, а с другой — могут быть полезны широкому кругу пользователей, доступны через встроенные C++ библиотеки.



Для подключения встроенных C++ библиотек используется механизм С++ UDF.

* [DateTime](datetime.md)
* [Digest](digest.md)
* [Histogram](histogram.md)
* [Hyperscan](hyperscan.md)
* [Ip](ip.md)
* [Math](math.md)
* [Pcre](pcre.md)
* [Pire](pire.md)
* [Protobuf](protobuf.md)
* [Re2](re2.md)
* [String](string.md)
* [Unicode](unicode.md)
* [Url](url.md)
* [Yson](yson.md)

<!---
[comment]: <> Перегенерировать основу списка: grep 'udf/list' index.yml | grep -v index | tr -d \': | awk '{print "* ["$2"](../../"$3")";}'
-->
