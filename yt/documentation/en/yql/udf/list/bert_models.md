---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/bert_models.md
sourcePath: yql-product/udf/list/bert_models.md
---
**BertModels** is a set of functions to handle models in the bert_models format. Colloquial language often refers to models in this format as ".htxt" models.

Learn more about the library that supports them here: https://a.yandex-team.ru/arc/trunk/arcadia/quality/relev_tools/bert_models/README.md

In short, this is a method to store different types of model to make their "unified" application possible.

It was initially developed as a part of a project to embed BERT-like models in the search stack. At first, it was barely an offline contour for formula selection, but later the same format became used for delivering data to runtime.

Key properties of models in this format:
1. self-sufficiency, "delivery as a comprehensive object"
2. ability to test and compare offline and online contours, "reduce difs"
3. abstraction of contents from the interface
4. an attempt to do this without reducing productivity


**NOTE**: In most cases, a model is deemed to be a black box where inputs are fed as ```input name -> text``` and ```output name -> float ```is obtained as a result.

Maintainers: https://staff.yandex-team.ru/departments/yandex_search_tech_quality_factor_dep28474/ , ilnurkh@ , https://nda.ya.ru/t/r3ALAqSR5pb97n

### Main examples:
1. most of that what isn't documented here should be reflected in tests at https://a.yandex-team.ru/arc/trunk/arcadia/quality/relev_tools/bert_models/udfs/tests/cases. Make sure to look there.

2. [Apply](https://cluster-name.yql/Operations/X2H5Ai--PAdX140Qev5wLDIJi2T-JhD1PEtLFTzR6wk=)
   ```(yql)
   use hahn;

   PRAGMA yt.DefaultMemoryLimit="30G";

   PRAGMA File("bert_model_1368513368.bert.htxt", "https://proxy.sandbox.yandex-team.ru/1368513368");

   PRAGMA File("libbert_models_udf.so", "yt://hahn/home/ranking/prod_build_artifacts_storage/latest_libbert_models_udf.so");
   PRAGMA udf("libbert_models_udf.so");

   $input = AsList(
       AsTuple("QueryNormed", "some query"),
       AsTuple("OmniTitleNormed", "some title"),
       AsTuple("OmniTitle_SOME", "SOME title"),
   );

   $model = BertModels::LoadModel(FilePath("bert_model_1368513368.bert.htxt"));


   SELECT BertModels::Apply(
       $model,
       $input
   );
   ```

2. [Apply batch](https://cluster-name.yql/Operations/X2H6m53udrYiCjuUag7MAA2a3My9Wfjk6WU3WpBW-14=)
   ```(yql)
   PRAGMA File("libbert_models_udf.so", "yt://hahn/home/ranking/prod_build_artifacts_storage/latest_libbert_models_udf.so");
   PRAGMA UDF("libbert_models_udf.so");

   PRAGMA yt.DefaultMemoryLimit = "40G";

   PRAGMA File("bert_model_tested.htxt", "https://proxy.sandbox.yandex-team.ru/1710545815");

   $bert_model_tested = "bert_model_tested.htxt";

   $input_rows = [
       <|
           Features:[
               ("QueryBertNormed", "вк"),
               ("MainContentUrlNormedV1BertNormed", "vk")
           ],
           PassThrough: 1
       |>,
       <|
           Features:[
               ("QueryBertNormed", "ok"),
               ("MainContentUrlNormedV1BertNormed", "ok")
           ],
           PassThrough: 2
       |>
   ];

   SELECT PassThrough, ToDict(Result) as results
   FROM (
       PROCESS AS_TABLE($input_rows) as pool
       USING BertModels::ApplyBatchStream(
               BertModels::LoadModel(
                   FilePath($bert_model_tested),
                   '{"MaxBatchSize": 2}'
               ),
               TableRows()
           )
       )
   ORDER BY PassThrough;
   ```

* MaxBatchSize is optional. It can affect memory consumption.

#### Sub-batches

For use with consideration of sub-batches (see the library document at https://a.yandex-team.ru/svn/trunk/arcadia/quality/relev_tools/bert_models/README.md), a 3rd field can be added to the fields to be passed. Besides Features and PassThrough, SubBatchesDivisor string can be passed

- batches are still formed dynamically
- if there are several samples with identical SubBatchesDivisor in a batch, they will be added to one and the same SubBatch
- unfortunately, it is still needed to pass common sub-batch data in each sample
- the user needs to ensure that these data are identical (in nodes, sub-batch data will be retrieved from the first document in the sub-batch)

<!--3. [Model comparison on proto-pools in web](https://cluster-name.yql/Operations/X2DN4pdg8hJMWQNj8vxYisaIbin3XOzHzDR4BPJ-Ovg=)-->



### Use on Gpu
1. ```UseGPU``` option key is available in LoadModel. For correctly arcadiazed implementations, you can set this key and force job scheduling in containers with gpu.

2. Correct layers are needed for tf models

3. More information is available at https://a.yandex-team.ru/svn/trunk/arcadia/quality/relev_tools/bert_models/README.md

4. Example
```(yql)
use hahn;

PRAGMA yt.QueryCacheMode="disable";

PRAGMA File("libbert_models_udf.so", "yt://hahn/home/ranking/prod_build_artifacts_storage/latest_libbert_models_udf.so");
PRAGMA UDF("libbert_models_udf.so");

--PRAGMA yt.CoreDumpPath = "//tmp/grechnik/coredump";
PRAGMA yt.Pool="research_gpu";
PRAGMA yt.PoolTrees = "gpu_tesla_a100";
PRAGMA yt.OperationSpec = '{mapper = {gpu_limit = 1; layer_paths = [
    "//home/ranking/prod_build_artifacts_storage/latest_tf-executor-plugin";
    "//home/ranking/prod_build_artifacts_storage/cudnn-8.0.5-cuda11.1";
    "//porto_layers/delta/gpu/cuda/11.4";
    "//porto_layers/delta/gpu/driver/450.119.04";
    "//porto_layers/cached_in_tmpfs/bionic_base"
]; }; }';

PRAGMA yt.DefaultMemoryLimit="64G";

$tf_id = "2021/09/boyalex/2404187597"; --tf
-- https://yt.yandex-team.ru/hahn/navigation?path=//home/ranking/prod_berts_storage/models/2021/09/boyalex/2404187597/meta.json
$ynmt_id = "2021/09/boyalex/2405042847"; --ynmt
--  https://yt.yandex-team.ru/hahn/navigation?path=//home/ranking/prod_berts_storage/models/2021/09/boyalex/2405042847/meta.json

$tf_url="yt://hahn/home/ranking/prod_berts_storage/models/" || $tf_id || "/bert.htxt";
$ynmt_id="yt://hahn/home/ranking/prod_berts_storage/models/" || $ynmt_id || "/bert.htxt";


PRAGMA File("tf.htxt", $tf_url);
PRAGMA File("ynmt.htxt", $ynmt_id);

$input = [
    ("OmniTitleBertNormed", "вконтакте"),
    ("QueryBertNormed", "vk"),
    ("OmniUrlNormedV1BertNormed", "https vk com"),
];

INSERT INTO @tmp1
SELECT
    "tf" as model,
    BertModels::Apply(
        BertModels::LoadModel(
            FilePath("tf.htxt"),
            '{"UseGPU": true, "MaxBatchSize":1}',
            ["libtf_executor_so.so"]),
        $input
    ) as resGpu,
    BertModels::Apply(
        BertModels::LoadModel(FilePath("tf.htxt")),
        $input
    ) as res
FROM `//home/factordev/ilnurkh/yqlres/imgintent_check` LIMIT 1; -- just to force use yt and mapper-spec from PRAGMA yt.OperationSpec


INSERT INTO @tmp2
SELECT
    "ynmt" as model,
    BertModels::Apply(
        BertModels::LoadModel(FilePath("ynmt.htxt"), '{"UseGPU": true, "MaxBatchSize":1}'),
        $input
    ) as resGpu,
    BertModels::Apply(
        BertModels::LoadModel(FilePath("ynmt.htxt")),
        $input
    ) as res

FROM `//home/factordev/ilnurkh/yqlres/imgintent_check_loc` LIMIT 1; -- just to force use yt and mapper-spec from PRAGMA yt.OperationSpec
COMMIT;

SELECT * FROM @tmp1
UNION ALL
SELECT "note - this is not paired models, it is expected to have different predicts" as model
UNION ALL
SELECT * FROM @tmp2;
```

<!--[Operation Run Example](https://cluster-name.yql/Operations/Y00TLbq3k1vJNHKRQr6BHrxUA24AnEMqLWn7pdXcXmQ=)-->

**NOTE**: Always use BatchStream variants of Apply because this is the only way to obtain normal performance.

Be careful: it's strongly recommended to use commits to divide application preparation, application, and post-processing. This is because using cpu operations and having an idle Gpu means that cards are tied in for no reason.

### How to teach and make models
1. To learn about a FTMoon project about fine-tune process facilitation for a specific task, see https://a.yandex-team.ru/arc/trunk/arcadia/quality/relev_tools/ftmoon/README.md
2. Visit gotmanov@, boyalex@, and maybe ilnurkh@

### Description of functions

* Creates an object for tokenization from the MakeInputTransformer dictionary

* DoInputTransformation performs tokenization with the same input format as Apply This function's result can be fed to ApplyOnTokenized and ApplyBatchStreamOnTokenized

* LoadModel — opens the model file and gets the file path and opening options (see section of the same name)

* LoadModelConfig — creates config object from model files

* LoadModelConfigJson — returns a Json string of config proto-buff obtained from a model file

* MakeModelConfig — creates a config object from string proto-text

* GetConfigJson — returns a model's .htxt config as a string with json

* GetModelGraphDescription — outputs (in a non-structured form) the tf-graph composition

* RestrictInput — converts a list of value-input pairs according to model config Applies normalizations.

* RestrictInputConfig — converts a list of value-input pairs according to model config. Applies normalizations (unlike RestrictInput, receives a Json string of a model's config proto-buff instead of a config resource indicator).

* RestrictInputByTransformationMaker — same as RestrictInput, but works with TransformationMaker only, rather than with a full model

* FetchInfoFromProtoPool — fetches data from proto pool as string pairs
* FetchInfoFromRequests — fetches data from request preprocessing in as string pairs

* DescribeTokenization — describes input tokenization in a non-structured form

* MakeInputTransformerFromModel and MakeInputTransformerFromModelJsonConfig — create an object to apply tokenization from a dictionary inside a model (and, optionally, new tokenization config). If no new config is set, tokenization is equivalent to internal tokenization inside a model at textual inputs. I.e., Apply(model) = ApplyOnTokenized(Tokenize overTMakeInputTransformerFromModel(model, NULL))

* MakeInputTransformerFromTxtVocab, MakeInputTransformerFromTxtVocabJsonConfig — create TransformationMaker from a dictionary file and transformation config

* ReLoadModelUnsafe — reopens model with another config. The first argument is a model, the second is a config in proto-text format

* Family of Apply functions: Apply, ApplyForCustomLayers, ApplyOnTokenized, ApplyForCustomLayersOnTokenized, ApplyBatchStream, ApplyBatchStreamOnTokenized, ApplyForCustomLayersBatchStream, ApplyForCustomLayersBatchStreamOnTokenized

* NormalizeInput — tests input normalization on a piece from config. (see examples in tests).

* RemapValue — tests remap's predicate on a piece from config. (see examples in tests).

* UrlTokenizeV1, UrlTokenizeVNaive, FetchQfufTopOriginal, RegionToGeoNamesRus_Beta, RegionToGeoNamesRusV1 — normalizations of the same name moved to individual functions
* UnpackBertApplyInfoInDocInfo — unpacks a document's binary data in average response (or in proto pool)

### LoadModel options
* UseGPU — allows use on GPU mode
* DeviceIndex — selects video card id
* InterOpThreads — TF applier's parameter of the same name
* IntraOpThreads — TF applier's parameter of the same name
* CustomExtractableLayers — list of strings with layer names (they can be viewed via GetModelGraphDescription) that must be extracted in ApplyForCustomLayers family of functions. Only supported for tensor-flow models.
* MaxBatchSize — YNMT applier's parameter of the same name that defines the actual batch size at the ynmt layer (using a larger BatchSize at the application stage will have no effect)
* NumThreads — number of YNMT applier's threats


### ApplyBatchStream options
see examples at https://a.yandex-team.ru/svn/trunk/arcadia/quality/relev_tools/bert_models/udfs/tests/cases

* BatchSize — batch size at the yql layer
* ExternalThreads — number of batches applied in parallel
* QueueSizeLimit — number of batches for which data are loaded but no result is returned
* Ordered — whether the sequence of documents will be observed. Can slow down in case of parallel processing.



