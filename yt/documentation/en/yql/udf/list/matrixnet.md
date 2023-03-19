---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/matrixnet.md
sourcePath: yql-product/udf/list/matrixnet.md
---
# Matrixnet UDF
The Udf is designed to apply [Matrixnet](https://wiki.yandex-team.ru/jandekspoisk/kachestvopoiska/matrixnet/) models.

### Model loading
```yql
Matrixnet::LoadModel(
  String is the model path in the info format
) -> Resource<Matrixnet.Model> -- model pointer
```

### Applying one formula
```yql
Matrixnet::Apply(
  Resource<Matrixnet.Model>, -- result of Matrixnet::LoadModel,
  List<Float>, -- factor list (float)
  String -- Factor slices
) -> Double -- application result
```
<!--[Example](https://cluster-name.yql/Operations/W-XBPzo4QYuSD97LEud9gOsHe8q61eNgQaAp3kScLps=)-->

### Applying a formula list
```yql
Matrixnet::MultiApply(
  List<Resource<Matrixnet.Model>>, -- model list,
  List<Float>, -- factor list (float)
  String -- Factor slices
) -> List<Double> -- result of applying all the formulae in the order they appear in the input list
```
<!--[Example](https://cluster-name.yql/Operations/W-XDA53udkPRHdlX_rNbeCXSnr_tkUd3j5OB2PpmFqU=)-->

### Applying one formula in batch mode
```yql
Matrixnet::ApplyBatch(
  Resource<Matrixnet.Model>, -- result of Matrixnet::LoadModel,
  List<
    Struct<
        Features:List<Float>, -- factor list (float)
        PassThrough:* -- arbitrary data that are passed through the function without change
    >
  >,
  String, -- Factor slices
  Int? -- number of entries in the batch
) -> List<
  Struct<
    Result:Float, -- model application result
    PassThrough:* -- passed to the PassThrough argument without changes
  >
> -- application result
```
<!--[Example](https://cluster-name.yql/Operations/W-XF7J3udkPRHdokZgim_tf4RqwK8647jzmyLNvJU5Y=)-->

### Using a formula list in batch mode
```yql
Matrixnet::MultiApplyBatch(
  List<Resource<Matrixnet.Model>>, -- model list,
  List<
    Struct<
        Features:List<Float>, -- factor list (float)
        PassThrough:* --  arbitrary data that are passed through the function without change
    >
  >,
  String, -- Factor slices
  Int? -- number of entries in the batch
) -> List<
    List<
      Struct<
        Result:Float, -- model application result
        PassThrough:* -- passed to the PassThrough argument without change
      >
    >
> -- result of applying all the formulae in the order they appear in the input list
```
<!--[Example](https://cluster-name.yql/Operations/W-XGl2im9QCG1bVzNZLSjeWV_8X253A9jJ0aooG81IM=)-->

### Working with trees
```yql
Matrixnet::SubModel(
  Resource<Matrixnet.Model>, -- result of Matrixnet::LoadModel,
  ui32, ui32 -- tree numbers [left, right)
) -> Resource<Matrixnet.Model> -- submodel that comprises the specified trees

Matrixnet::SubModel(
  Resource<Matrixnet.Model>, -- result of Matrixnet::LoadModel,
  ui32 -- step size
) -> List<Resource<Matrixnet.Model>> -- list of submodels, each consisting of <step size> trees (the last model can be smaller)
-- the result of combining applications of submodel is equal to the application of the source model
```

[Example](https://a.yandex-team.ru/arc/trunk/arcadia/yql/udfs/ml/matrixnet/test/cases/Submodels.sql)

