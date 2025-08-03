# GeoHash UDF
Набор функций для кодирования и декодирования географических координат в [GeoHash](https://en.wikipedia.org/wiki/Geohash).

``` yql
GeoHash::Encode(Double{Flags:AutoMap}, Double{Flags:AutoMap}, [Uint8?]) -> Uint64
GeoHash::TryEncode(Double{Flags:AutoMap}, Double{Flags:AutoMap}, [Uint8?]) -> Uint64?
GeoHash::EncodeToString(Double{Flags:AutoMap}, Double{Flags:AutoMap}, [Uint8?]) -> String
GeoHash::TryEncodeToString(Double{Flags:AutoMap}, Double{Flags:AutoMap}, [Uint8?]) -> String?
```

Аргументы:
1. Широта;
2. Долгота;
3. Точность (от 1 до 12 включительно, по умолчанию 12).

**Примеры**:

``` yql
SELECT GeoHash::Encode(1.2345, 6.7890);           -- 865288303859193463
SELECT GeoHash::EncodeToString(1.2345, 6.7890);   -- "s0hz42xx7pmr"
```

``` yql
GeoHash::Decode(Uint64{Flags:AutoMap}, [Uint8?]) -> Struct<'Lat':Double,'Lon':Double>
GeoHash::TryDecode(Uint64{Flags:AutoMap}, [Uint8?]) -> Struct<'Lat':Double,'Lon':Double>?
GeoHash::DecodeFromString(String{Flags:AutoMap}) -> Struct<'Lat':Double,'Lon':Double>
GeoHash::TryDecodeFromString(String{Flags:AutoMap}) -> Struct<'Lat':Double,'Lon':Double>?
```

Обратное преобразование к координатам.
Опциональный аргумент точность от 1 до 12 включительно, по умолчанию 12.

**Примеры**:

``` yql
SELECT GeoHash::Decode(1018585775999650093ul);    -- <|'Lat':1.23, 'Lon':123.456|>
SELECT GeoHash::DecodeFromString("w8pycrcvm29e"); -- <|'Lat':1.23, 'Lon':123.456|>
```

``` yql
GeoHash::GetBitsNeighbours(Uint64{Flags:AutoMap}, [Uint8?]) -> Struct<'East':Uint64?,'North':Uint64?,'NorthEast':Uint64?,'NorthWest':Uint64?,'South':Uint64?,'SouthEast':Uint64?,'SouthWest':Uint64?,'West':Uint64?>
GeoHash::GetStringNeighbours(String{Flags:AutoMap}) -> Struct<'East':String?,'North':String?,'NorthEast':String?,'NorthWest':String?,'South':String?,'SouthEast':String?,'SouthWest':String?,'West':String?>
```

Получение соседей по 8ми направлениям.
Опциональный аргумент точность от 1 до 12 включительно, по умолчанию 12.

**Примеры**:
``` yql
SELECT GeoHash::GetBitsNeighbours(1018585775999650093ul);
SELECT GeoHash::GetStringNeighbours("w8pycrcvm29e");
```

``` yql
SELECT GeoHash::GetBitsNeighbours(GeoHash::Encode(1.2345, 6.7890));
SELECT GeoHash::GetStringNeighbours(GeoHash::EncodeToString(1.2345, 6.7890));
```

``` yql
SELECT GeoHash::DecodeFromString("w8pycrcvm29e");
```
