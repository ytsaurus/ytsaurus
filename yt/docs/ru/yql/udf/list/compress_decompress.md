# Compress UDF и Decompress UDF
Набор распространённых алгоритмов компрессии и декомпрессии данных:

{% if audience == "internal" %}
``` yql
Compress::Gzip(String{Flags:AutoMap}, Uint8) -> String
Compress::Zlib(String{Flags:AutoMap}, Uint8) -> String
Compress::Lz4(String{Flags:AutoMap}, [Uint16?]) -> String
Compress::Brotli(String{Flags:AutoMap}, Uint8) -> String
Compress::Lzma(String{Flags:AutoMap}, Uint8) -> String
Compress::BZip2(String{Flags:AutoMap}, Uint8) -> String
Compress::Snappy(String{Flags:AutoMap}) -> String
Compress::Zstd(String{Flags:AutoMap}, Uint8) -> String
Compress::Xz(String{Flags:AutoMap}) -> String
```
``` yql
Decompress::Gzip(String{Flags:AutoMap}) -> String
Decompress::Zlib(String{Flags:AutoMap}) -> String
Decompress::Lz4(String{Flags:AutoMap}) -> String
Decompress::Brotli(String{Flags:AutoMap}) -> String
Decompress::Lzma(String{Flags:AutoMap}) -> String
Decompress::BZip2(String{Flags:AutoMap}) -> String
Decompress::Snappy(String{Flags:AutoMap}) -> String
Decompress::Zstd(String{Flags:AutoMap}) -> String
Decompress::Xz(String{Flags:AutoMap}) -> String
```
``` yql
TryDecompress::Gzip(String{Flags:AutoMap}) -> String?
TryDecompress::Zlib(String{Flags:AutoMap}) -> String?
TryDecompress::Lz4(String{Flags:AutoMap}) -> String?
TryDecompress::Brotli(String{Flags:AutoMap}) -> String?
TryDecompress::Lzma(String{Flags:AutoMap}) -> String?
TryDecompress::BZip2(String{Flags:AutoMap}) -> String?
TryDecompress::Snappy(String{Flags:AutoMap}) -> String?
TryDecompress::Zstd(String{Flags:AutoMap}) -> String?
TryDecompress::Xz(String{Flags:AutoMap}) -> String?
```

{% else %}
``` yql
Compress::Gzip(String{Flags:AutoMap}, Uint8) -> String
Compress::Zlib(String{Flags:AutoMap}, Uint8) -> String
Compress::Brotli(String{Flags:AutoMap}, Uint8) -> String
Compress::Lzma(String{Flags:AutoMap}, Uint8) -> String
Compress::BZip2(String{Flags:AutoMap}, Uint8) -> String
Compress::Snappy(String{Flags:AutoMap}) -> String
Compress::Zstd(String{Flags:AutoMap}, Uint8) -> String
Compress::Xz(String{Flags:AutoMap}) -> String
```
``` yql
Decompress::Gzip(String{Flags:AutoMap}) -> String
Decompress::Zlib(String{Flags:AutoMap}) -> String
Decompress::Brotli(String{Flags:AutoMap}) -> String
Decompress::Lzma(String{Flags:AutoMap}) -> String
Decompress::BZip2(String{Flags:AutoMap}) -> String
Decompress::Snappy(String{Flags:AutoMap}) -> String
Decompress::Zstd(String{Flags:AutoMap}) -> String
Decompress::Xz(String{Flags:AutoMap}) -> String
```
``` yql
TryDecompress::Gzip(String{Flags:AutoMap}) -> String?
TryDecompress::Zlib(String{Flags:AutoMap}) -> String?
TryDecompress::Brotli(String{Flags:AutoMap}) -> String?
TryDecompress::Lzma(String{Flags:AutoMap}) -> String?
TryDecompress::BZip2(String{Flags:AutoMap}) -> String?
TryDecompress::Snappy(String{Flags:AutoMap}) -> String?
TryDecompress::Zstd(String{Flags:AutoMap}) -> String?
TryDecompress::Xz(String{Flags:AutoMap}) -> String?
```
{% endif %}

#### Примеры

``` yql
SELECT Compress::Brotli("YQL", 8);                          -- "\x0b\x01\x80\x59\x51\x4c\x03"
SELECT Decompress::Brotli("\x0b\x01\x80\x59\x51\x4c\x03");  -- "YQL"
SELECT TryDecompress::Brotli("YQL"); -- NULL
```
