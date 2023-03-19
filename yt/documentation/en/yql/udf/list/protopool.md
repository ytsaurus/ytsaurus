---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/protopool.md
sourcePath: yql-product/udf/list/protopool.md
---
# ProtoPool UDF
Basic operations with factor pools.

Maintainer: <a href="https://staff.yandex-team.ru/ilnurkh">Ilnur Khiziev</a>

You can:

1. convert a string from a proto or tsv pool to a yql structure with basic fields
2. convert all proto fields into json and then process it, for example, using Python
3. rewrite basic fields in the protopool
4. add features based on slices

{% cut "Refresher on terminology" %}


* Pool (usually a search pool) is a dataset used to teach ranking or other formulae. It's comprised of:
   1. query ID
   2. URL (document ID)
   3. assessor's pair rating (target/Rating)
   4. factors used to describe this pair in the system
   5. (optionally) metadata about factors — slicesString

* protopool — [a pool string](https://a.yandex-team.ru/arc/trunk/arcadia/kernel/idx_proto/feature_pool.proto). Is stored in mapreduce with a 4-byte prefix. In addition to features, it includes other information about the given query-document pair. Good pools are stored as protopools.
* tsv format — yamr-key stores RequestId, yamr-value uses tsv to store the rating, URL, grouping (it seems nobody uses it anymore), and factors. The format isn't extensible, but it's extremely popular because it can be conveniently processed in awk.
* [Slices](https://wiki.yandex-team.ru/jandekspoisk/kachestvopoiska/factor-slices) is a very useful thing that everybody always forgets to pass through correctly, take into account, etc.

{% endcut %}

An auxiliary structure is used:
```yql
-- A structure with the main pool fields
TSmallPoolLine = Struct<
  RequestId: ui64,
  Rating: float,
  Url: String,
  SlicesString: String,
  Features: List<float>
>
```

Function list:

```yql
ProtoPool::ParseMrProtoLine(TString?{Flags:AutoMap}) -> TSmallPoolLine? -- Unpackages the main fields
ParseMrProtoLineExpanded -> Struct -- it also returns RankingUrl, Grouping
ProtoPool::ParseMrTsvLine(TString?{Flags:AutoMap), TString?{Flags:AutoMap)) -> TSmallPoolLine? -- Unpackages the main fields from a tsv pool
-- converts protobuf into json. For example, it lets you redirect a custom logic to Python without the hassle of linking modules for reading protopools.
ProtoPool::ToJsonString(String?{Flags:AutoMap) -> String?
-- Packages data into the proto format. The second optional argument is interpreted as a proto string that will accept fields from TSmallPoolLine
ProtoPool::ToMrProtoLine(TSmallPoolLine?{Flags:AutoMap}, TString?) -> TString?
-- factor list + complete protobuf that needs to have its intersecting fields replaced.
FeaturesTuple = Tuple<List<float>, String>
-- Appends factors to the end of the specified slice and returns a new factor array and slice description for this array.
ProtoPool::AppendFeatures(FeaturesTuple?{Flags:AutoMap} InputFeatures, String sliceToAppendNewFeatures, List<float>? NewFeatures ) -> FeaturesTuple
ProtoPool::ParseMrTsvLineToProtoLine(TString?{Flags:AutoMap}) -> TString? -- Converts yamr-value between proto-tsv, retains grouping
ProtoPool::SaveProtoLineAsTsvLine(TString?{Flags:AutoMap}, TString?{Flags:AutoMap}) -> TString? -- Converts yamr-value between proto-tsv, retains grouping
ProtoPool::SwitchProtoTarget(TString?{Flags:AutoMap}, Double) -> TString? -- Replaces the Rating field in ProtoLine with the field in the second argument. Narrowing can occur because the protopool description uses Float.
ProtoPool::AppendFeaturesToProtoString(String ProtoString, String sliceToAppendNewFeatures, List<float>? NewFeatures, bool? CheckFeatures) -> String -- Writes features to the given slice and returns a proto string. Optionally checks whether features fall within the [0, 1] range,
ProtoPool::StripProtoPoolForFormula(String) -> String -- Removes all the fields not used to teach functions from the serialized protoboof
-- Serializes and deserializes ProtoLine <-> Yson
ProtoPool::ToYsonString(String) -> String
ProtoPool::FromYsonString(String) -> String
-- Changes factors for slices
ProtoPool::TransformFeaturesToSlices(TString?{Flags:AutoMap}, TString?) -> TString? -- Converts the Feature from the first argument with ProtoLine to new slices from the second argument and trims or pads with zeros
ProtoPool::TransformFeaturesToSlicesFromArray(List<Float>, String, String) -> List<Float> -- Converts features from the first argument with slices from the second argument to slices from the third argument
ProtoPool::TransformFeaturesToSlicesSmallPoolLine(TSmallPoolLine, TString?) -> TString? -- Converts the Feature from the first argument with ProtoLine to new slices from the second argument and trims or pads with zeros
ProtoPool::UnwrapTurboUrl — converts the turbo URL to the source URL, but doesn't change other URLs
ProtoPool::ReplaceFeatures(TString, `List<Struct(SliceName: String, IndexInSlice: i32, NewValue: float)>`, bool?) -- Replaces features from the protopool. The third argument indicates whether slices can be extended/added in the source protopool.
```

## Examples:

ProtoPool::ParseMrProtoLine and ProtoPool::ParseMrTsvLine commands return a structure that holds a Features array.


### 1. Get a factor by name

To get a feature index by the feature name, slice, and slicesString, you can use ```SELECT FactorsInfo::GetAbsoluteIdBySlicesId(SlicesString, SliceName, FactorsInfo::GetFactorInfoByName(SliceName, FeatureName).IdInSlice)```.
```

PRAGMA File("latest_libmulti_udf.so", "yt://hahn/home/ranking/prod_build_artifacts_storage/latest_libmulti_udf.so");
PRAGMA UDF("latest_libmulti_udf.so");

SELECT FactorsInfo::GetFactorInfoByName("begemot_query_factors", "IsCSQuery");

SELECT FactorsInfo::GetAbsoluteIdBySlicesId(
    "web_production[0;1923) web_l2[1923;1995) web_production_formula_features[1995;2000) begemot_query_factors[2000;2300) begemot_query_rt_l2_factors[2300;2332) rapid_clicks_l2[2332;2334) begemot_model_factors[2334;2349)",
    "begemot_query_factors",
    FactorsInfo::GetFactorInfoByName("begemot_query_factors", "IsCSQuery").IdInSlice
);

---- Result1:
(
    "IdInSlice": 296,
    "IsImplemented": false,
    "IsUnknown": false,
    "MaxValue": 1,
    "MinValue": 0,
    "Name": "IsCSQuery",
    "Slice": "begemot_query_factors",
    "Tags": [
        "TG_UNUSED",
        "TG_THEME_CLASSIF",
        "TG_DYNAMIC",
        "TG_UNIMPLEMENTED",
        "TG_QUERY_ONLY",
        "TG_NEURAL"
    ]
)
---- Result2
2296
```
<!--[Example in web interface](https://cluster-name.yql/Operations/YRPZGXcszIFrHAj2mY6l5Csqw8bf5CIubosWAPi-OVY=)-->

```

$getFeature($value) -> {
    $slice = "begemot_query_factors";
    $idInSlice = FactorsInfo::GetFactorInfoByName("begemot_query_factors", "IsCSQuery").IdInSlice;
    $parsed = ProtoPool::ParseMrProtoLine($value);
    $absId = FactorsInfo::GetAbsoluteIdBySlicesId(
        $parsed.SlicesString,
        $slice,
        $idInSlice
    );
    return $parsed.Features[$absId];
};

```

&nbsp;

```yql
SELECT ProtoPool::ParseMrTsvLine(key, value) AS MainFields -- TOBEDONE
SELECT ProtoPool::ParseMrProtoLine(value) AS MainFields -- TOBEDONE |
SELECT ProtoPool::ToMrProtoLine(ProtoPool::ParseMrProtoLine(value)) -- TOBEDONE
SELECT ProtoPool::ToMrProtoLine(ProtoPool::ParseMrProtoLine(value), value) As ProtoLineMerged -- TOBEDONE
SELECT ProtoPool::ToJsonString(value) as JsonStringOfProtoPool -- TOBEDONE
SELECT ProtoPool::ToYsonString(value) as YsonStringOfProtoPool
SELECT ProtoPool::FromYsonString(value) as ProtolineInString
SELECT ProtoPool::TransformFeaturesToSlices(value, "web_production[0;1402)") as TransformedLine
SELECT ProtoPool::TransformFeaturesToSlicesFromArray(features, "web_production[0;1402)", "web_production[0;1402)") as TransformedFeatures
SELECT ProtoPool::TransformFeaturesToSlicesSmallPoolLine(value, "web_production[0;1404)", "web_production[0;1402)") as TransformedSmallLine
SELECT ProtoPool::AppendFeaturesToProtoString(value, "web_l3_mxnet", AsList(0.1f, 0.2f)) as TransformedLine
SELECT ProtoPool::StripProtoPoolForFormula(value) as StrippedProtoPoolForFormula -- Обрезает поля для использования только в формуле, на момент релиза совпадает с ExpandedProtoLine

SELECT ProtoPool::ToMrProtoLine(
  AsStruct(
    COALESCE(ProtoPool::AppendFeatures(
    AsTuple(AsList(0.f, 0.2f, 0.5f), "lingboost_beta[0;3)"), "web_production", AsList(0.7f, 0.8f)
  ).0, AsList(0.f)) As Features,
  COALESCE(ProtoPool::AppendFeatures(
    AsTuple(AsList(0.f, 0.2f, 0.5f), "lingboost_beta[0;3)"), "web_production", AsList(0.7f, 0.8f)
    ).1, "") as SlicesString,
  "a.ru" as Url,
  0.1f as Rating,
  15 as RequestId
  )
) as MrConverted -- "bAAAAAgPFc3MzD06BGEucnViFDMzMz/NzEw/AAAAAM3MTD4AAAA/ogJGYWxsWzA7NSkgZm9ybXVsYVswOzIpIHdlYlswOzIpIHdlYl9wcm9kdWN0aW9uWzA7MikgbGluZ2Jvb3N0X2JldGFbMjs1KQ=="
```

<!--[Example in web interface](https://cluster-name.yql/Operations/WUOc2AcJUf-fpCTEfWMahg8byyok0XBDlrQ9uFwySOw=)-->
