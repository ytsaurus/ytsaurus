---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/factorsinfo.md
sourcePath: yql-product/udf/list/factorsinfo.md
---
# FactorsInfo UDF
Resolves ranking factor names in Search. The Get functions (except for GetAbsoluteIdBySlicesId) return a structure that includes: Name (factor name)
* Slice (the slice the factor sits in)
* IdInSlice (the factor index in the slice)
* Tags (the list of factor tags)
* IsImplemented (whether the factor has defined the implementation-time)
* IsUnknown is the flag indicating that udf lacks factor data (usually because it was built from the revision that lacked the factor in the register).

{% note info "Note" %}

Note that functions return data relevant at the moment of udf building.

{% endnote %}

Maintainer: <a href="https://staff.yandex-team.ru/lightater">Dmitry Plotnikov</a>


* GetFactorInfoByName — to get factor data using its slice and name.
* GetFactorInfoByInnerIndex — to get factor data using its slice and slice index.
* GetFactorInfoByIndex — to get factor data using SlicesString and SlicesString index.
* GetAbsoluteIdBySlicesId — to get factor index in slicesString using SlicesString, slice, and slice index.

<!--Examples:

* <https://cluster-name.yql/Operations/XUQ2yQlcTmE55MX5jUDnUxd_r5ZUngjzrnxxKv7CvcM=>-->

```
SELECT FactorsInfo::GetFactorInfoByName("rapid_clicks", "DoppUrlThreshold30Decay30Prior0");
```

Let's define the following structure: FeaturesSlices = (Features: List`<float>`, Slices: String). Features is a set of feature values. Slices is a border-string.

* UnpackFeatures — use FeaturesSlices to get a list of pairs (slice, feature value vector) List`<Tuple<String, List<float>>>`.
* PackFeatures — use a list of pairs (slice, feature value vector) to get a FeaturesSlices protopool structure
* ReplaceFeatures — replace values in FeaturesSlices (possibly lets you extend the slices from within). Returns a new FeaturesSlices value. Arguments:
   - Input: FeaturesSlices — the source structure
   - ReplaceItems: List`<Struct(SliceName: String, IndexInSlice: i32, NewValue: float)>` — the list of necessary replacements. The IndexInSlice factor in the SliceName slice accepts the value NewValue.
   - AllowIncreaseSlices: Optional<bool> — a flag that indicates whether slices should be extended (added) to the source structure if the necessary factor is missing. If a slice is added/extended, all new factors (except for the replaced one) are filled with zeroes. If a slice needs to be added/extended but AllowIncreaseSlices equals false, the function fails. By default, false.

<!--Examples:

* <https://cluster-name.yql/Operations/XwRvsGim9dl0V8c2VC6JksZHDygN4_f2MyOjcsUs8z0=>-->
