# CatBoost UDF
Применение [CatBoost](https://catboost.yandex) моделей.

```yql
CatBoost::LoadModel(
  String -- путь к файлу с моделью
) -> Resource<CatBoost.TFormulaEvaluator> -- указатель на готовую к применению модель

CatBoost::EvaluateBatch(
  Resource<Catboost.TFormulaEvaluator>, -- результат CatBoost::LoadModel
  List<
    Struct<
      FloatFeatures:List<Float>, -- числовые фичи
      CatFeatures:List<String>, -- категориальные фичи
      PassThrough:* -- произвольные данные, которые пропускаются через функцию без изменений
    >
  >,
  Uint32? -- TargetCount, число целей в модели, по умолчанию 1
  Uint32? -- BatchSize, по умолчанию 128
) -> List<
  Struct<
    Result:List<Float>, -- результат применения модели
    PassThrough:* -- переданное в аргумент PassThrough без изменений
  >
> -- результат
```

## Пример использования

**Задача:** применить модель CatBoost, чтобы угадать значение столбца Target на основе значений других столбцов. Модель обучена на данных, похожих на данные входной таблицы.

{% cut "Пример данных входной таблицы `catboost_data`" %}

#|
||	**Age** |	**Cat1** |	**Cat2** |	**Cat3** |	**Children** |	**Education** |	**Float1** |	**Float2** |	**Float3** |	**Float4** |	**Gender** |	**Group** |	**Income** |	**MaritalStatus**	 | **Nationality** |	**Occupation** |	**String** |	**Target** ||
||	19 |	"0"	 | "n"|	"1" |	"Own-child" |	"Some-college" |	10 |	0 |	0 |	40 |	"Male" |	"?" |	208874 |	"Never-married" |	"White" |	"?" |	"United-States" |	"1"||
||	27 |	"0"	 |"n"	| "1"	| "Not-in-family"	| "Some-college" |	10	| 0	| 0	| 40 |	"Female"	| "Private"	| 158647|	"Never-married"	| "White"	| "Adm-clerical"	| "United-States"	| "1"||
||	18 |	"0" |	"n"	| "1"	| "Unmarried"	| "10th" |	6|	0|	0|	40 |	"Male"	| "Private" |	115258|	"Never-married"	| "White"	| "Craft-repair"	| "United-States"	| "1"||
||	49 |	"0" |	"n"	| "1"	| "Unmarried"	| "Assoc-voc" |	11|	0	|1380|	42|	"Male"	| "Private"|	141944|	"Married-spouse-absent"	| "White"	| "Handlers-cleaners"	| "United-States"	| "1"||
||	37 |	"0" |	"n"	| "1"	| "Husband"	| "5th-6th"|	3|	0|	0	|40|	"Male"	| "Private"|	227128|	"Married-civ-spouse"	| "White"	| "Craft-repair"	| "United-States"	| "1"||
||	28 |	"0" |	"n"	| "1"	| "Unmarried"	| "HS-grad"|	9|0	|0	|55|	"Male"	| "Private"|	22422|	"Never-married"	| "White"	| "Transport-moving"	| "United-States"	| "1"||
||	28 |	"0"	 |"n"	| "1"	| "Husband"	| "HS-grad"|	9|	0|	0	|45|	"Male"	| "Private"|	190367|	"Married-civ-spouse"	| "White"	| "Machine-op-inspct"	| "United-States"	| "1"||
||	20 |	"0" |	"n"	| "1"	| "Own-child"	| "Some-college"|	10|	0	|0	|36|	"Male"	| "?"|	287681|	"Never-married"	| " White"	| "?"	| "United-States" |	"1"||
||	43 |	"0" |	"n"	| "1"	| "Unmarried"	| "Some-college"|	10|	0	|0	|38|	"Female"	| "Private"|	196158	|"Divorced"	| "White"	| "Adm-clerical" |	"United-States"	| "1"||
||	30 |	"0" |	"n"  |	"1" |	"Own-child" |	"HS-grad" |	9 |	0 |	0 |	40 |	"Male" |	"Private" |	112650 |	"Divorced" |	"White" |	"Craft-repair" |	"United-States" |	"1" ||
|#

{% endcut %}

```yql
PRAGMA file(
    "model.bin",
    "<путь к файлу с моделью>"
);

$evaluator = CatBoost::LoadModel( -- инициализируем CatBoost с заданной моделью
    FilePath("model.bin")
);

-- подготовка данных
$data = (SELECT
    [ -- числовые данные складываются в список List<Float>
        Age,
        Income,
        Float1,
        Float2,
        Float3,
        Float4
    ] AS FloatFeatures,
    [   -- категориальные значения — в список List<String>
        Cat1,
        Cat2,
        Cat3,
        `Group`,
        Education,
        MaritalStatus,
        Occupation,
        Children,
        Nationality,
        Gender,
        `String`
    ] AS CatFeatures,
    Target AS PassThrough -- в batch-режиме CatBoost можно передавать любые значения, как есть
FROM catboost_data);

$processed = (PROCESS $data
USING CatBoost::EvaluateBatch( -- провести оценку модели в batch-режиме на указанных выше данных
    $evaluator,                
    TableRows()
));

SELECT
    Result[0] > 0.5 AS Guess, -- у этой модели одна цель,
                              -- поэтому результирующий список состоит только из одного элемента
    PassThrough == "1" AS Answer, -- PassThrough содержит значение столбца Target
    Result[0] - 0.5 AS Score
FROM $processed;
```
