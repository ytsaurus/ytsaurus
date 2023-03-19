---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/dssm.md
sourcePath: yql-product/udf/list/dssm.md
---
# DSSM UDF
Application of [DSSM](https://wiki.yandex-team.ru/jandekspoisk/kachestvopoiska/relevance/dssm/).
```yql
Dssm::LoadModel(
  String, -- model path in the applier format
  String? -- retrieve a submodel that computes only this output
) -> Resource<DSSM.Model> -- model pointer

Dssm::Apply(
  Resource<DSSM.Model> -- result of Dssm::LoadModel,
  Struct<String:*> -- data for the model and model field names, e.g.: "mary goes home" AS query, "http://yandex.ru" AS url
  String -- required model output name
) -> List<Float> -- application result, similar to nn_applier
```

<!--[Example](https://cluster-name.yql/Operations/WhQLjydq_S6S0yrtryuTWEdvvYnK_cS4ZxUFVqxpuBE=)-->
