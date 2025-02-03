
# Functions of built-in C++ libraries

Many application functions that on the one hand are too specific to become part of the YQL core, and on the other hand might be useful to a wide range of users, are available through built-in C++ libraries.



Embedded C++ libraries are linked using UDFs in C++.


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
