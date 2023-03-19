---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/requestbundle.md
sourcePath: yql-product/udf/list/requestbundle.md
---
# RequestBundle UDF
ReqBundle is a query description format that can be used for indices and text machine. This udf lets you unpackage reqbundle from cgi (qbundle), merge them, apply restricts, and collect statistics.

Maintainers: ilnurkh

* FetchQbundlesFromCgi is a qbundle list (`List<String>`) from the cgi query (String).
* MergeQbundles merges several qbundles into one `List<String> -> String`.
* RestrictQbundle applies a json restrict to qbundle and returns the resulting qbundle. [Example](https://a.yandex-team.ru/arc/trunk/arcadia/yql/udfs/quality/request_bundle/test/cases/requests.sql?rev=3193545#L16)
* GetQbundleStats uses qbundle to return statistics (size, number of blocks and queries, plus statistics for each query type). See example below.
* UnpackQbundleHr unpackages qbundle to display which queries of what weight and type are present in qbundle. See example below.
* MakeBundleFromTexts creates reqbundle from the specified text set with type and weight labels. Note that resulting qbundles don't have word forms. Their application is limited — you can't pass the result to base, etc.
* CalcFeaturesByForwardIndex receives qbundle as input (lemmatized in all cases), a list of texts with weights, and a list of extension types that are used to calculate factors of the text machine.
* LemmatizeBundle lemmatizes the input qbundle and adds word forms to it.

**Examples**

<!--* <https://cluster-name.yql/Operations/WdyjPlEZjTgRRdjpe9ji2eO_ge2Px7l56Mw9CrjAQIo>
* <https://cluster-name.yql/Operations/X5shFmim9cB5qYzV7t7zeSgfG7VSMillW94wXMLPlVY=>-->


```
SELECT
     RequestBundle::MakeBundleFromTexts(
         AsList(
             AsStruct(
                 "expansions text" as Text,
                 0.1f as Weight,
                 "Qfuf" as TypeStr
             ),
             AsStruct(
                 "other expansions text" as Text,
                 0.5f as Weight,
                 "XfDtShow" as TypeStr
             )
         )
     )
;
```
