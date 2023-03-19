---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/blender_factor_storage.md
sourcePath: yql-product/udf/list/blender_factor_storage.md
---
# BlenderFactorStorage UDF
Functions for working with blender factors [Wiki](https://wiki.yandex-team.ru/jandekspoisk/kachestvopoiska/blender/features/flagi-dlja-upravlenijami-blendernymi-faktorami/#logirovanieblendernyxfaktorov)

Maintainers — [blender team](https://abc.yandex-team.ru/services/blndr/)

## Compression, decompression of factors
These functions are intended to handle the factors logged by ApplyBlender pre-ranging to reqans log, to `Upper.ApplyBlender.compressed_factors` props.

### BlenderFactorStorage::Compress
Serializes and compresses blender factors.

```yql
-- Arguments: — First: set of static factors
--            — Second: dictionary of dynamic factors
BlenderFactorStorage::Compress(List<float>, Dict<String, List<float>>) -> String
```

### BlenderFactorStorage::Decompress
De-serializes compressed blender factors.

```yql
-- Arguments — First: String — string with compressed blender factors
BlenderFactorStorage::Decompress(String) -> Struct<
 StaticFactors: List<float>,
 DynamicFactors: Dict<String, List<float>>,
 IsError: bool
>
```

{% note info "Attention!" %}

Check IsError flag for successful decompression

{% endnote %}

<!--[Example in web interface](https://cluster-name.yql/Operations/X2SmBS--PAdX2rZQMrVHJxbKKWSCyzAq68p93ceoWME=)

[Parsing example from nanosessions](https://cluster-name.yql/Operations/X6pulpdg8t08nZv3sXiwKFwoeGsNSE3bU4xydIWxJGo=)-->

