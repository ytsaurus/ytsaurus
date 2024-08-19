# TensorFlow UDF

Применение [TensorFlow](https://www.tensorflow.org) графов вычислений.

``` yql
TensorFlow::InitSession(
  String, -- путь к сериализованному в protobuf формате графу вычислений, единственный обязательный аргумент
  Bool?, -- EnableMKLDNN, включение MKLDNN https://01.org/mkl-dnn
  Bool?, -- MKLCBWRCompatible, включение совместимости с SSE2 для MKL https://software.intel.com/en-us/mkl
  Bool?, -- MKLDNNUseWeightsAsGiven, по умолчанию false
  Bool?, -- AllowSoftPlacement, по умолчанию true
  Int32?, -- IntraOpParallelismThreads, по умолчанию 0
  Int32?, -- InterOpParallelismThreads, по умолчанию 0
) -> Resource<TensorFlow.Session> -- указатель на TensorFlow сессию с проинициализированным
                                  -- графом вычислений

TensorFlow::RunBatch(
  Resource<TensorFlow.Session>, -- результат TensorFlow::InitSession
  List<Struct<
    PassThrough:* -- произвольные данные, которые пропускаются через функцию без изменений
    *:* -- имена остальных полей соответствуют именам входов графа вычислений, которые
        -- будут переданы в TensorFlow, а их типы данных — соответствующим формам тензоров;
        -- примитивный тип данных соответствует скалярному тензору, список — вектору,
        -- матрицы на данный момент не поддерживаются.
  >>,
  Type<Struct<*:*>>, -- описание выходов графа, которые необходимо вычислить по аналогии со входами
  List<String>?, -- TargetNodeNames, опциональный список имен узлов графа, которые нужно вычислить,
                 -- но не возвращать
  Uint32?, -- BatchSize, по умолчанию 128
  Uint32?, -- MinVectorSize, по умолчанию 1, позволяет выроврять входные вектора до указанной длины,
           -- если на входе они имеют разную длину <= указанной.
           -- Если не выставлен, длины векторов батча выравниваются до максимальной в рамках батча.
) -> List<Struct<
  PassThrough:* -- переданное в аргумент PassThrough без изменений,
  *:* -- остальные поля соответствуют типу данных, указанному в третьем аргументе
>> -- результат
```

`TensorFlow::RunBatch` является достаточно сложной полиморфной функцией, предназначенной для выполнения в [PROCESS](../../syntax/process.md). Настоятельно рекомендуется ознакомиться с примером ниже.

## Пример использования

**Задача:** угадать пол владельца заданного адреса электронной почты с помощью TensorFlow и предварительно обученной нейронной сети LSTM.

{% cut "Пример данных входной таблицы `emails_sample`" %}

#|
|| **email** |	**uid**||
||	"ayse.betul@list.ru" |	8401912741484409541||
||	"gkorchigina@advisegroup.ru" |	7443540151492739379||
||	"derunova@hyva.ru" |	1206408241455208802||
||	"n.tohtaeva@apex-realty.ru" |	8401934471495213920||
||	"danya_kuzya11@topservice.od.ua" |	9354031741499148496||
||	"e.a.abrosimova@hyva.ru" |	1206415791497489033||
||"muratguney@tut.by" |	84019341499566593||
||"juriy647@zakrepi.ru" | 7443595151476837533||
||"inginiringstroy@bodo.ua" |	120641461502424396||
||	"homuncule@fenixrostov.ru" |	8401933951498412781||
||	"vetal1984@72.ru" |	7443588691497078993||
||	"botagoz@game-forest.com"	 |1206414471493466146||
||	"kent2010@etvp.perm.ru" |	8401933051457890411||
||	"gordeeva@lider-exp.ru" |	7443585201492797494||
||	"a.vakish@tut.by" |	1206414051498009965||
||	"d.volia.pinsk@b-urist.ru" |	8401932351484247832||
||	"juravleva.e@rambler.ru" |	7443577411470618706||
||	"hr.pers.specialist7@prizyvanet.ru" |	1206412411504975868||
||	"sales106@belydom.ru" |	8401929831460458776||
||	"r.zinatullin@smum.biz" |	7443574961501375635||
||	"dpm.spec.delivery.rent2@mgllojistik.com" |	1206412411502352831||
||	"ekaterina@rc.yartel.ru" |	8401921921456190987||
||	"alex.krause@aktivdengi.com" |	7443572251501358851||
||	"acc.invest.other@smpvo.ru" |	1206412411498937658||
||	"turris.secretary-r@n-h-s.ru" |	8401916371502363695||
|#

{% endcut %}

```yql
PRAGMA yt.DataSizePerJob = "128M"; -- поскольку это вычисление достаточно трудоемкое,
                                   -- будем запрашивать небольшой размер данных на задание
PRAGMA file("email.pb", "<путь к сериализованному графу вычислений>");
$session = TensorFlow::InitSession(
    FilePath("email.pb") -- путь к определению графа TensorFlow
);

PRAGMA library("char_table.sql"); -- считываем прикрепленный файл как источник для импорта
IMPORT char_table SYMBOLS $char_to_float; -- импортируем лямбду из библиотеки, которая преобразует символы
                                          -- электронной почты таким же образом, как это было сделано
                                          -- во время обучения модели

--лямбда-функция для предобработки адресов электронной почты
$email_to_float_vector = ($email) -> {
    $cut = FIND($email, "@"); -- оставляем только логин
    $cut = MIN_OF($cut, 20u) ?? 20u;
    $email = SUBSTRING($email, 0, $cut);  -- обрезаем слишком длинные логины
    $email_bytes = String::ToByteList($email); -- преобразуем строку в список байтов
    RETURN ListFlatMap($email_bytes, $char_to_float); -- применяем лямбду
};

$input = (
    SELECT
        $email_to_float_vector(email) AS input_1, -- имя столбца должно совпадать с именем
                                                  -- входа графа TensorFlow
        TableRow() AS PassThrough -- специальное имя столбца, чтобы передать
                                  -- любые данные в batch-режиме TensorFlow;
                                  -- в данном примере передается целая входная строка
    FROM emails_sample);
--------------------------------------------------------------------------------
-- Соглашения между TensorFlow и YQL:
-- - имя входного столбца должно совпадать с именем входного графа, а его тип должен
-- соответствовать форме тензора и типу данных;
-- - поддерживаемые типы данных: Float, Double, Int32, Int64, Uint8, Bool;
-- - List<> означает вектор, в то время как скаляр, матрицы и другие формы
-- пока не поддерживаются;
-- - у модели в этом примере только один вход, но их может быть много;
-- - те же правила применяются и к выходному графу;
-- - столбец PassThrough в настоящее время является обязательным, но может стать
-- опциональным в будущем.
--------------------------------------------------------------------------------

$processed = (PROCESS $input
USING TensorFlow::RunBatch( -- Apply TensorFlow graph in batches
    $session,     
    TableRows(), -- специальное имя для итератора по строкам таблицы
    -- описание вывода графа для вычисления
    Struct<network_output:List<Float>>,
    -- каждый элемент структуры представляет один из выходных данных графа;
    -- имя каждого элемента должно соответствовать имени, которое использовалось
    -- во время процесса обучения;
    -- соответствие между тензорами и типами данных YQL такое же, как и у входных данных
    ListCreate(String),   -- TargetNodeNames
    128,                  -- BatchSize
    20                    -- MinVectorSize
));

SELECT
    network_output[0] ?? 0.0f AS score, -- результат попытки угадать пол по электронной почте:
                                        -- - близко к 0.0 — "женский"
                                        -- - близко к 1.0 — "мужской"
                                        -- - близко к 0.5 — "трудно определить"
    PassThrough.email AS email, -- исходная электронная почта
    PassThrough.uid AS uid -- отдельный столбец для дополнительных входных данных
FROM $processed;
```
