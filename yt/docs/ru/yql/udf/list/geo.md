# Geo UDF

Набор функций на основе библиотеки [geobase6](https://wiki.yandex-team.ru/geotargeting/libgeobase). О логике работы функций см. документацию по библиотеке, в частности, по классу [NGeobase::TLookup](https://doc.yandex-team.ru/lib/libgeobase5/concepts/interfaces-cplusplus-lookup.xml).

Важно понимать, что в любой конкретный момент времени Geo-UDF функциями используется максимально свежий файл данных (```geodata6.bin```). Что это значит? Свойства какого-то IP-адреса или региона на момент пользовательского запроса могут выглядеть совсем не так, как будет видно в Вашем YQL-запросе, спустя неделю или месяц. Это же верно и для любых запросов к логам из прошлого. Хорошая новость - на это поведение можно влиять. Прагма [yt.GeobaseDownloadUrl](https://yql.yandex-team.ru/docs/yt/syntax/pragma#ytgeobasedownloadurl) позволяет подключать произвольный ресурс к запросу, чтобы он использовался в качестве источника гео-данных. Подробнее об этом (с примерами запросов) [рассказывается в публикации](https://clubs.at.yandex-team.ru/geotargeting/993) клуба "Определения местоположения".

## Geo::Region...
``` yql
Geo::RegionById(Int32?) -> Struct
Geo::RegionByIp(String?) -> Struct
Geo::RegionByIpNumeric(Uint32?) -> Struct
Geo::RegionByLocation(
    Double?, -- latitude
    Double?  -- longitude
) -> Struct
```

По переданному идентификатору региона, IP-адресу или координатам, соответственно, возвращает структуру со следующими полями (на момент написания), соответствующими методам `geobase5::region`:

* `String?`:
    * name
    * en\_name
    * short\_en\_name
    * iso\_name
    * native\_name
    * synonyms
    * official\_languages
    * widespread\_languages
    * timezone\_name
    * zip\_code
* `Bool?`:
    * is\_main
* `Int32?`:
    * type
    * id
    * capital\_id
    * geo\_parent\_id
    * parent\_id
    * population
    * position
    * zoom
* `Uint32?`:
    * services
* `Double?`:
    * latitude
    * latitude\_size
    * longitude
    * longitude\_size

[Документация по этим полям в самой библиотеке](https://doc.yandex-team.ru/lib/libgeobase5/concepts/interfaces-cplusplus-structures.html).

#### Примеры

``` yql
SELECT Geo::RegionById(1).en_name;         -- "Moscow and Moscow region"
SELECT Geo::RegionByIp("81.19.76.8").type; -- [3]
```

## Geo::RoundRegion...
``` yql
Geo::RoundRegionById(Int32?, String) -> Struct
Geo::RoundRegionByIp(String?, String) -> Struct
Geo::RoundRegionByIpNumeric(Uint32?, String) -> Struct
Geo::RoundRegionByLocation(
    Double?, -- latitude
    Double?, -- longitude
    String   -- scale
) -> Struct
```

Полностью идентичны аналогичным функциям без префикса Round с единственным отличием: «округляют» регионы до масштаба, указанного вторым аргументом (возвращают родительский регион указанного уровня). Допустимые значения второго аргумента:

* earth
* continent
* world\_part
* country
* district
* region
* city
* town

#### Примеры

``` yql
SELECT Geo::RoundRegionById(1, "continent").en_name;           -- "Eurasia"
SELECT Geo::RoundRegionByIp("81.19.76.8", "district").en_name; -- "Central Federal District"
```

## Geo::Get...
``` yql
Geo::GetParents(Int32?) -> List<Int32>
Geo::GetChildren(Int32?) -> List<Int32>
Geo::GetSubtree(Int32?) -> List<Int32>
Geo::GetPath(String?) -> List<Int32>
Geo::GetTimezone(Int32{Flags:AutoMap}) -> String
Geo::TryGetTimezone(Int32{Flags:AutoMap}) -> String?
Geo::GetAsset(String{Flags:AutoMap}) -> String?
Geo::GetChiefRegionId(Int32{Flags:AutoMap}) -> Int32

-- для функций ниже в аргументе IP-адрес
Geo::GetIspNameByIp(String?) -> String
Geo::GetOrgNameByIp(String?) -> String
```
#### Примеры

``` yql
SELECT Geo::GetParents(121000);   -- [121000; 1; 3; 225; 10001; 10000]
SELECT Geo::GetChiefRegionId(1))  -- [213]

SELECT Geo::GetIspNameByIp('77.88.8.8');  -- "yandex llc"
SELECT Geo::GetOrgNameByIp('8.8.8.8');    -- "google"
```

## Geo::Is...
``` yql
Geo::IsIpInRegion(String?, Int32) -> Bool
Geo::IsRegionInRegion(Int32?, Int32) -> Bool

-- для функций ниже в аргументе IP
Geo::IsYandex(String?) -> Bool -- NB-1: истина вернётся для любой Я-сети
Geo::IsYandexStaff(String?) -> Bool -- NB-2: Я-сотрудники из офисных сетей определяется с помощью только этой функции
Geo::IsYandexTurbo(String?) -> Bool
Geo::IsHosting(String?) -> Bool
Geo::IsProxy(String?) -> Bool
Geo::IsTor(String?) -> Bool
Geo::IsVpn(String?) -> Bool
Geo::IsMLVpn(String?) -> Bool -- проверка наличия у адреса VPN-признака, предсказанного ML-моделью, подробности в https://st.yandex-team.ru/CRYPTA-17583
Geo::IsAnonymousIp(String?) -> Bool -- NB-3: обобщающая проверка адреса на анонимность; функция вернёт истину, если установлен любой из флажков Proxy, Hosting, Vpn, ML-Vpn, Tor.
Geo::IsMobile(String?) -> Bool

-- для функций ниже в аргументе Id региона
Geo::IsEU(Int32?) -> Bool  -- вернёт, входит ли регион в Еврозону или нет
```

#### Примеры

``` yql
SELECT Geo::IsIpInRegion("81.19.76.8", 123); -- false
SELECT Geo::IsRegionInRegion(213, 1);        -- true
SELECT Geo::IsEU(203);                       -- true
SELECT Geo::IsEU(94);                        -- false
```

## Прочее
``` yql
Geo::FindCountry(Int32{Flags:AutoMap}) -> Int32 -- По идентификатору региона возвращает идентификатор страны,
                                                -- в которой он находится. В отличие от Geo::RoundRegionById
                                                -- возвращает только идентификатор, а не всю информацию целиком.
Geo::CalculatePointsDifference(
  Double{Flags:AutoMap}, -- latitude 1
  Double{Flags:AutoMap}, -- longitude 1
  Double{Flags:AutoMap}, -- latitude 2
  Double{Flags:AutoMap}  -- longitude 2
) -> Double -- дистанция в метрах
```

#### Примеры

``` yql
SELECT Geo::FindCountry(1);                                        -- 225
SELECT Geo::CalculatePointsDifference(10.10, 20.20, 30.30, 40.40); -- 3069740.779
```
