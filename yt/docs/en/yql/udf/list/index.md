
# Functions of built-in C++ libraries

Many application functions that on the one hand are too specific to become part of the YQL core, and on the other hand might be useful to a wide range of users, are available through built-in C++ libraries.


Embedded C++ libraries are linked using UDFs in C++.

* [Clickhouse](clickhouse.md)
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
* [Streaming](streaming.md)
* [String](string.md)
* [Tensorflow](tensorflow.md)
* [Unicode](unicode.md)
* [Url](url.md)
* [Yson](yson.md)


<!---
[comment]: <> Перегенерировать основу списка: grep 'udf/list' index.yml | grep -v index | tr -d \': | awk '{print "* ["$2"](../../"$3")";}'
-->
