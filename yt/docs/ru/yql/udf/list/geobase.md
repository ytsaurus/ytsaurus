# Geobase UDF

{% note info "" %}

**Geobase** &mdash; это переосмысленная версия [Geo-UDF](geo.md)-модуля.

{% endnote %}


## Основные отличия от Geo::

* при создании объекта для работы с геобазой, можно прописать все желаемые настройки, например, какой файл данных нужно использовать;
* в ОДНОМ скрипте можно использовать сразу НЕСКОЛЬКО объектов Geobase, с разными настройками;
* список функций в модуле более полно повторяет [API](https://a.yandex-team.ru/arcadia/geobase/include/lookup.hpp?rev=r14185012#L70-135) базовой версии [libgeobase](https://wiki.yandex-team.ru/geotargeting/libgeobase), и лишь частично пересекается с функциями прошлого Geo-модуля.


{% note info "О свежести файла данных" %}

Свойства IP-адресов и регионов могут изменяться с течением времени.

Поэтому важно помнить о согласованности данных геобазы и времени пользовательского запроса в таблицах YT-логов (получасовки, суточные, etc.) Как найди ссылку на нужный вам ресурс с геобазой было подробно рассказано в публикации "[как правильно подключать geodata6.bin](https://clubs.at.yandex-team.ru/geotargeting/993)" при работе с историческими логами, в клубе [Определение местоположения](https://clubs.at.yandex-team.ru/geotargeting).

{% endnote %}


## Инициализация гео-объекта Geobase::Init()

``` yql
Geobase::Init(
    'DatafilePath':String,         -- ресурс с геобазой, из FilePath(), обязательный параметр
    'ConfigPath':String?,          -- конфигурационный файл библиотеки, NOTA BENE: пока(!) не используется
    'TzdataPath':String?,          -- ресурс с архивом данных таймзон
    'DisputedConfigPath':String?,  -- конфиг спорных территорий
    'IsoPOV':String?               -- точка зрения на дерево регионов "по умолчанию", если не задано &mdash; будет "ru" для обратной совместимости
) -> Struct<Callable1, ...>        -- структура с функциями для вызова из скрипта
```

Большинство параметров размечено необязательными, их можно не указывать при вызове. С одним очевидным исключением &mdash; без `geodata*.bin` файла (см. параметр `DatafilePath`), полезную работу выполнить вряд ли получится.

{% note info "IsoPOV-параметр" %}

Это способ влияния на организацию дерева регионов. Дерево могло быть статичным, но есть спорные территории, принадлежность которых к странам зависит от "точки зрения". Подробности можно изучить по следующим ссылкам:
* [операция "Транслокальность"](https://wiki.yandex-team.ru/geotargeting/libgeobase/TranslocalMigration/) _(2014 год)_,
* [транслокальная единая геобаза](https://wiki.yandex-team.ru/geotargeting/libgeobase/translocal.ad/)  _(2024 год)_.
Любой неизвестный код будет заменён значением по умолчанию (`"un"` &mdash; точка зрения ООН).

{% endnote %}

**Пример**
``` yql
$geodatafile_path = FilePath("${some-path-to-geodata}");

-- эквивалентные варианты
$geo = Geobase::Init($geodatafile_path);
$geo_with_named_arg = Geobase::Init($geodatafile_path as DatafilePath,);
```

{% note info "" %}

Во всех дальнейших примерах предполагается, что инициализация `$geo`-объекта геобазы уже произведена.

{% endnote %}

### Настройка гео-объекта

Для удобства использования, все необходимые ресурсы уже доступны в YT-каталоге геобазы на hahn-кластере:
[yt://hahn/home/geotargeting/public/geobase](https://yt.yandex-team.ru/hahn/navigation?path=//home/geotargeting/public/geobase)

Инициализации $geo-объекта стандартным бинарным файлом данных `geodata6.bin` выглядит так:
``` yql
--
$default_data_url_prefix = "yt://hahn/home/geotargeting/public/geobase/";
$geodata6_fname = "geodata6.bin";
$geodata6_yt_path = $default_data_url_prefix || $geodata6_fname;

PRAGMA File($geodata6_fname, $geodata6_yt_path);
$geo = Geobase::Init(FilePath($geodata6_fname));
```
К сожалению, на текущий момент времени не получится спрятать это под капот модуля.

## Группы функций модуля

Функции в модуле можно условно разделить на несколько групп, в зависимости от того, с какими объектами данных они работают:
* регион и его свойства, лингвистики, местоположение в дереве,
* свойства IP-адреса,
* свойства таймзон,
* вспомогательные функции.

{% note info "IsoPOV-параметр в функциях" %}

У некоторых функций присутствует параметр `isoPOV` _(если при вызове он не указан, будет использовано `IsoPOV`-значение, заданного при создании `$geo`-объекта)_. Задав значение параметра при вызове функции, можно "на лету" переключить точку зрения на дерево регионов. Список доступных точек зрения можно узнать с помощью функции `GetKnownIsoPovList()`.

**Пример**
``` yql
$crimea_id = 977;

$crimea_reg_in_ru = $geo.GetRegionById($crimea_id, "ru");
$crimea_reg_in_ua = $geo.GetRegionById($crimea_id, "ua");

select $crimea_reg_in_ru.parent_id != $crimea_reg_in_ua.parent_id as different_parents;
```

{% endnote %}

### Работа с данными региона

#### `GetRegionBy*, GetRoundedRegionBy*` &mdash; получение всех свойств региона

Получение свойств региона по идентификатору или IP-адресу:
``` yql
$geo.GetRegionById('regId':Int32,['isoPOV':String?])->Struct<..reg-fields..>
$geo.GetRegionByIp('ip':String,['isoPOV':String?])->Struct<..reg-fields..>
```

Поиск региона по геопозиции:
``` yql
$geo.GetRegionByLocation('lat':Double,'lon':Double,['tuneToNearest':Bool?,'isoPOV':String?])->Struct<..reg-fields..>
```
По умолчанию `tuneToNearest` равно `false`. Если в параметре `tuneToNearest` указать значение `true`, будет сделана попытка привести геоточку к ближайшему населённому пункту.

``` yql
$geo.GetRoundedRegionById('regId':Int32, 'typeName':String, ['isoPOV':String?])->Struct<..reg-fields..>
$geo.GetRoundedRegionByIp('ip':String, 'typeName':String, ['isoPOV':String?])->Struct<..reg-fields..>
```

Аналогичны функциям без префикса, но работают с "округлением" региона до родительского региона.
Допустимые значения второго аргумента (альтернативные значения &mdash; через запятую, в скобках &mdash; [базовый числовой код](https://a.yandex-team.ru/arcadia/geobase/include/structs.hpp?rev=r14183633#L36-54) региона геобазы):
* continent (1)
* world\_part, worldpart (2)
* level-0, country (3)
* level-1, country\_part, country\_district (4)
* level-2, region (5)
* level-3, district (10)
* city (6)
* village, town (7)

{% note warning %}

Имеется неполное соответствие ранее использованным именам в Geo-модуле.

{% endnote %}

##### Struct-ответ
Все функции возвращают структуру со следующими полями региона
_(могут быть пустыми)_:
``` yql
Struct<
    'capital_id':Int32?,
    'city_id':Int32?,
    'en_name':String?,
    'id':Int32?,
    'is_eu':Bool?,                         // NB: Geo::IsEU()
    'is_main':Bool?,
    'iso_alpha3':String?,                  // *new
    'iso_name':String?,
    'latitude':Double?,
    'latitude_size':Double?,
    'longitude':Double?,
    'longitude_size':Double?,
    'name':String?,
    'native_name':String?,
    'official_languages':List<String>?,    // NB, list of ISO
    'parent_id':Int32?,
    'phone_code':String?,
    'population':Int32?,
    'services':List<String>?,              // *new, see GetKnownServices()
    'short_en_name':String?,
    'synonyms':List<String>?,
    'timezone_name':String?,
    'type':Int32?,
    'widespread_languages':List<String>?,  // NB, list of ISO
    'zip_code':String?,
    'zoom':Int32?
>
```

##### Примеры
``` yql
$city_type = 6;

$msk_id = 213;
$ru_id = 225;

-- case-1
$msk_reg = $geo.GetRegionById($msk_id);
select $msk_reg.type == $city_type as is_city_type;

-- case-2
$rounded_city2country_reg = $geo.GetRoundedRegionById($msk_id, "country");
select $rounded_city2country_reg.id == $ru_id as in_ru_country;
```

{% note info "О других полезных функциях" %}

Для упрощения поиска страны по региону можно использовать функцию `GetCountryId()`.

Родительские регионы других типов легко находятся с помощью функций `GetParentIdWithType*()`.

Также полезной может оказаться функция `IsRegionIdValid()`, которая проверит наличие региона в подключённом файле данных.

{% endnote %}

#### Работа с деревом

##### GetChildrenIds

Получение списка идентификаторов дочерних регионов.
``` yql
$geo.GetChildrenIds('regId':Int32,['isoPOV':String?])->List<Int32>
```

##### GetParentsIds

Получение списка идентификаторов родительских регионов по административно-территориальному делению (АТД).
``` yql
$geo.GetParentsIds('regId':Int32,['isoPOV':String?])->List<Int32>
```

##### GetGeoParentsIds

Получение списка идентификаторов родительских регионов с учётом географической вложенности. Например, по АТД город (`$city_id`) может входить сразу в Область (`$obl_id`). Но географически располагается внутри одноимённого района (`$distr_id`). Поэтому функция `GetParentsIds()` вернёт цепочку вида `[$city_id, $obl_id, ...]`, у функции `GetGeoParentsIds()` будет другой результат &mdash; `[$city_id, $distr_id, $obl_id, ...]`. Стоит заметить, что выдача функций для большинства регионов из нашего гео-справочника будет совпадать.
``` yql
$geo.GetGeoParentsIds('regId':Int32,['isoPOV':String?])->List<Int32>
```

##### GetTree

Получение идентификаторов дочерних регионов в дереве (DFS). Корнем будет заданный регион.
``` yql
$geo.GetTree('regId':Int32)->List<Int32>
```

##### GetCountryId, GetParentIdWithType*

Получение региона-родителя с заданным типом.
``` yql
$geo.GetCountryId('regId':Int32,['isoPOV':String?])->Int32
$geo.GetParentIdWithType('regId':Int32,['type':Int32?])->Int32
$geo.GetParentIdWithTypeName('regId':Int32,['typeName':String?])->Int32
```
В функции GetParentIdWithTypeName() ожидаются имена типов, аналогичные значениям в функции GetRoundedRegionBy*().

##### GetRegionsIdsByType

Получение списка идентификаторов регионов по заданному типу (только страны, например)
Список типов можно увидеть в таблице [geobase/region_type](https://yt.yandex-team.ru/hahn/navigation?path=//home/geotargeting/public/geobase/region_type)
``` yql
$geo.GetRegionsIdsByType('type':Int32,['isoPOV':String?])->List<Int32>
```

##### Is*InRegion

Проверки на вхождение региона в другой регион.
``` yql
IsIdInRegion('regId':Int32,'id':Int32,['isoPOV':String?])->Bool
IsIpInRegion('ip':String,'ip':String,['isoPOV':String?])->Bool
```


### Работа с лингвистиками

``` yql
$geo.GetLinguistics('regId':Int32,['langIso':String?])->Struct<..lings-fields..>
```

Выдает название региона в различных падежах для указанного языка в `langIso`-параметре. Если `langIso` не указан, то возвращается название региона на русском языке.

##### Struct-ответ
``` yql
Struct<
    'ablative':String?,
    'accusative':String?,
    'dative':String?,
    'directional':String?,
    'genitive':String?,
    'instrumental':String?,
    'locative':String?,
    'nominative':String?,
    'preposition':String?,
    'prepositional':String?,
    'reg_id':Int32?"
>
```

{% note info "О доступных лингвистиках" %}

Чтобы понять, какие языки доступны, можно воспользоваться вспомогательной функцией `GetKnownLinguistics()`

{% endnote %}


### Работа со свойствами IP-адреса

``` yql
$geo.GetIpTraits('ip':String)->Struct<..ip-fields..>
```

{% note info "Все свойства за один раз" %}

В модуле `Geo` было множество разрозненных функций для получения свойств IP-адреса. В новом модуле все эти вызовы были заменены единственным. Функция возвращает структуру со всеми полями:

{% endnote %}

##### Struct-ответ
``` yql
Struct<
    'asn_list':List<String>?,
    'is_anon':Bool?,           -- обобщённый признак анонимности
    'is_hosting':Bool?,        -- хостинг?
    'is_ml_vpn':Bool?,         -- признак VPN-а, подсчитанный ML-моделькой Крипты
    'is_mobile':Bool?,         -- мобильный?
    'is_proxy':Bool?,          -- прокси?
    'is_reliable':Bool?,       -- надёжная привязка (обычно взводится на Я-адресах)
    'is_reserved':Bool?,       -- зарезервированные подсети
    'is_stub':Bool?,           -- заглушка, данных нет
    'is_tor':Bool?,            -- TOR?
    'is_vpn':Bool?,            -- VPN?
    'is_yandex_net':Bool?,     -- будет взведён для любой Я-сети
    'is_yandex_staff':Bool?,   -- адрес из офисных Я-сетей (сотрудники)
    'isp_name':String?,        -- имя провайдера (если известно)
    'org_name':String?,        -- имя организации (если известно)
    'reg_id':Int32?            -- регион привязки
>
```

##### Примеры
``` yql
$ip_traits = $geo.GetIpTraits('::ffff:77.88.8.8');

select
    $ip_traits.is_yandex_net  -- true
    $ip_traits.org_name       -- "yandex"
;
```

### Работа с временными зонами

Группа функций для работы с временными зонами. Для их корректной работы нужно указать путь к архиву данных таймзон в параметре `TzdataPath` при инициализации объекта Geobase. У всех функций есть необязательный `whenTsInSec`-параметр, с помощью которого можно узнавать свойства временной зоны в конкретный момент времени. По умолчанию, используется текущее время.

{% note info "Инициализация $geo-объекта timezone-данными" %}

Необходимый ресурс с данными временных зон доступен в YT-каталоге геобазы на hahn-кластере:
[yt://hahn/home/geotargeting/public/geobase/tzdata.zip](https://yt.yandex-team.ru/hahn/navigation?path=//home/geotargeting/public/geobase/tzdata.zip)

Инициализации $geo-объекта:
``` yql
-- #1. предварительная подготовка

$default_data_url_prefix = "yt://hahn/home/geotargeting/public/geobase/";

$geodata6_fname = "geodata6.bin";
$tzdata_fname = "tzdata.zip";

$geodata6_yt_path = $default_data_url_prefix || $geodata6_fname;
$tzdata_yt_path = $default_data_url_prefix || $tzdata_fname;

PRAGMA File($geodata6_fname, $geodata6_yt_path);
PRAGMA File($tzdata_fname, $tzdata_yt_path);

-- #2. инициализация

$geo = Geobase::Init(
    FilePath($geodata6_fname) as DatafilePath,
    FilePath($tzdata_fname) as TzdataPath,
);
```

{% endnote %}

Все функции возвращают структуру с полями свойств зоны

##### Struct-ответ
``` yql
Struct<
    'abbr':String?,
    'dst':String?,
    'name':String?,
    'offset':Int64?
>
```

#### GetTimezoneName

``` yql
$geo.GetTimezoneName('tzName':String,['whenTsInSec':Int32?])->Struct<..tz-fields..>
```
Функция по имени зоны возвращает её свойства.

##### Примеры
``` yql
select $geo.GetTimezoneByName("Asia/Almaty").offset  -- 5 * 3600
```

#### GetTimezoneById

``` yql
$geo.GetTimezoneById('regId':Int32,['whenTsInSec':Int32?,'isoPOV':String?])->Struct<..tz-fields..>
```
Функция возвращает свойства временной зоны по коду региона.

#### GetTimezoneByLocation

``` yql
$geo.GetTimezoneByLocation('lat':Double,'lon':Double,['whenTsInSec':Int32?,'isoPOV':String?])->Struct<..tz-fields..>
```
Аналогично другим функциям группы, возвращает свойства временной зоны. Вначале по координатам восстанавливается регион и имя таймзоны. Если точка попадает мировой океан &mdash; таймзона будет приближённо восстанавливаться по долготе (_longitude_).


### Вспомогательные функции

#### GetDatafileTimestamp

``` yql
$geo.GetDatafileTimestamp()->Int32
```

Выдает время в unixtime-формате, когда был сгенерирован бинарный файл Геобазы, используемый в $geo-объекте.

#### GetLastUpdateTimestamp

``` yql
$geo.GetLastUpdateTimestamp()->Int32
```

Выдает время в unixtime-формате, когда была внесена последняя модификация в сведения о регионах, находящихся в бинарный файл Геобазы, который используется в $geo-объекте. Речь именно про секцию с деревом регионов и их свойств.

#### IsRegionIdValid

``` yql
$geo.IsRegionIdValid('regId':Int32)->Bool
```

Функция позволяет проверить наличие региона с указанным идентификатором в бинарном файле данных, используемом в $geo-объекте. Она может быть полезна для фильтрации обрабатываемых логов. Если функция вернула "истину" &mdash; регион существует. В противном случае возможны варианты. Либо в лог попал ошибочный код региона, либо в запросе используется устаревший файл данных.

Например, Вы обрабатываете лог от 15 ноября. Предположим, что 10 ноября в геосправочник был добавлен новый регион-город, код которого начал появляться в логах сервисов с 13 ноября. Если вы используете файл данных, собранный до 10 ноября, при обработке лога возникнут ошибки. Штамп времени генерации файла данных можно уточнить с помощью функции `GetDatafileTimestamp()`.

#### GetKnownLinguistics

``` yql
$geo.GetKnownLinguistics()->List<String>
```

Функция возвращает перечень языков, присутствующие в файле данных Геобазы, и которые можно передавать в функцию `GetLinguistics()` в качестве `langIso`-параметра.

**Пример**
``` yql
select "kk" in $geo.GetKnownLinguistics() as kk_is_known;  -- NB: kk for KZ
```

#### GetKnownServices

``` yql
$geo.GetKnownLinguistics()->List<String>
```

Функция возвращает список известных сервисов, которые могут быть указаны в свойствах регионов. См. `services`-поле в структуру ответа `GetRegionBy*()`-функций.

**Пример**
``` yql
select "subway" in $geo.GetKnownServices() as metro_in_list;
```

#### CalculateDistance

``` yql
Geobase::CalculateDistance('lat1':Double,'lon1':Double,'lat2':Double,'lon2':Double)->Double
```

Функция вычисляет расстояние в метрах между двумя точками на поверхности Земли.

### Транслокальность, спорные территории

Конфиг спорных территорий для Я-сервисов (Реклама, Метрика, КУБ-аналитика) располагается в YT-каталоге геобазы на hahn-кластере:
[yt://hahn/home/geotargeting/public/geobase/unified-disp-config.json](https://yt.yandex-team.ru/hahn/navigation?path=//home/geotargeting/public/geobase/unified-disp-config.json)

#### IsDisputedRegion

``` yql
$geo.IsDisputedRegion('regId':Int32)->Bool
```

Функция проверяет, является ли регион с указанным идентификатором частью спорной территории.

**Пример**
``` yql
$msk_id = 213;
$is_msk_disputed = $geo.IsDisputedRegion($msk_id);  -- false

$simfi_id = 146;
$is_simfi_disputed = $geo.IsDisputedRegion($simfi_id); -- true
```

#### GetKnownIsoPovList

``` yql
$geo.GetKnownIsoPovList()->List<String>
```
Функция возвращает список всех известных isoPOV-кодов из конфига спорных территорий, которые допустимо использовать при вызове $geo-функций.

#### GetLatestIsoPov

``` yql
$geo.GetLatestIsoPov()->String
```

Функция возвращает последний актуальный iso-код из конфига спорных территорий.

**Пример**
``` yql
-- если использовать конфиг для Я-сервисов (по состоянию на июнь 2024 года)
select $geo.GetLatestIsoPov();  -- "yd24"
```

## Обратная связь

Если в процессе использования данного модуля у вас появились вопросы или замечания, пожалуйста, сообщите об этом [с помощью наших ФОС](https://wiki.yandex-team.ru/geotargeting/#formyobratnojjsvjazi).
