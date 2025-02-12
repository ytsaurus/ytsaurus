# CatBoost UDF

Applying [CatBoost](https://catboost.yandex) models.

```yql
CatBoost::LoadModel(
  String -- path to file with the model
) -> Resource<CatBoost.TFormulaEvaluator> -- indicates a ready-to-use model

CatBoost::EvaluateBatch(
  Resource<Catboost.TFormulaEvaluator>, -- result CatBoost::LoadModel
  List<
    Struct<
      FloatFeatures:List<Float>, -- numerical features
      CatFeatures:List<String>, -- categorial features
      PassThrough:* -- arbitrary data passed through the function unchanged
    >
  >,
  Uint32? -- TargetCount, number of targets in the model. Default is 1
  Uint32? -- BatchSize, default is 128
) -> List<
  Struct<
    Result:List<Float>, -- model application result
    PassThrough:* -- passed to PassThrough argument unchanged
  >
> -- result
```

## Usage example

**Task:** apply CatBoost model that has been trained on data similar to input table to guess the value of Target column based on other column values.

{% cut "Input table `catboost_data` data example" %}

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
PRAGMA file( -- specify the URL where to get the model
    "model.bin",
    "<URL>"
);

-- Initialize CatBoost FormulaEvaluator with given model:
$evaluator = CatBoost::LoadModel(
    FilePath("model.bin")
);

-- Prepare the data:
$data = (SELECT
    [ -- Float features are packed into a List<Float>
        Age,
        Income,
        Float1,
        Float2,
        Float3,
        Float4
    ] AS FloatFeatures,
    [ -- Cat features are packed into List<String>
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
    Target AS PassThrough -- you can pass any values through
                          -- batch mode of CatBoost as is
FROM catboost_data);

$processed = (PROCESS $data
USING CatBoost::EvaluateBatch( -- Evaluate model in batch mode
    $evaluator,                -- on the data above
    TableRows()
));

SELECT
    Result[0] > 0.5 AS Guess, -- this model has one target,
                              -- so the Result list has only one item
    PassThrough == "1" AS Answer, -- the PassThrough contains the Target column
                                  -- value that has been passed to it
    Result[0] - 0.5 AS Score
FROM $processed;
```
