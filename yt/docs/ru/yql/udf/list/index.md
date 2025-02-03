# Функции встроенных C++ библиотек

Многие прикладные функции, которые с одной стороны слишком специфичны, чтобы стать частью ядра YQL, а с другой — могут быть полезны широкому кругу пользователей, доступны через встроенные C++ библиотеки.



Для подключения встроенных C++ библиотек используется механизм С++ UDF.

* [Hyperscan](hyperscan.md)
* [Pcre](pcre.md)
* [Pire](pire.md)
* [Re2](re2.md)
* [String](string.md)
* [Unicode](unicode.md)
* [DateTime](datetime.md)
* [Url](url.md)
* [Ip](ip.md)
* [Yson](yson.md)
* [Digest](digest.md)
* [Math](math.md)
* [Histogram](histogram.md)

<!---
[comment]: <> Перегенерировать основу списка: grep 'udf/list' index.yml | grep -v index | tr -d \': | awk '{print "* ["$2"](../../"$3")";}'
-->
