# TensorFlow UDF

Using [TensorFlow](https://www.tensorflow.org) computational graphs.
```yql
TensorFlow::InitSession(
  String, -- path to the computational graph serialized in the protobuf format (the only mandatory agrument)
  Bool?, -- EnableMKLDNN, enable MKLDNN https://01.org/mkl-dnn
  Bool?, -- MKLCBWRCompatible, enable SSE2 compatibility for MKL https://software.intel.com/en-us/mkl
  Bool?, -- MKLDNNUseWeightsAsGiven, false by default
  Bool?, -- AllowSoftPlacement, true by default
  Int32?, -- IntraOpParallelismThreads, 0 by default
  Int32?, -- InterOpParallelismThreads, 0 by default
) -> Resource<TensorFlow.Session> -- pointer to the TensorFlow session with an initialized computational graph

TensorFlow::RunBatch(
  Resource<TensorFlow.Session>, -- result of TensorFlow::InitSession
  List<Struct<
    PassThrough:* -- arbitrary data passed through the function without change
    *:* -- names of other fields correspond to computational graph input names that will be passed to TensorFlow,
        -- and fields' data types correspond to tensor forms; a primitive data type corresponds to a scalar
        -- tensor, a list — to a vector. Matrices are not supported at the moment.
  >>,
  Type<Struct<*:*>>, -- description of graph outputs that must be calculated like inputs
  List<String>?, -- TargetNodeNames, an optional list of graph node names that must be calculated, but not returned
  Uint32?, -- BatchSize, 128 by default
  Uint32?, -- MinVectorSize, 1 by default. Lets you adjust input vectors to the specified length
           -- if their input lengths differ and are less than or equal to the specified length.
           -- If the argument isn't specified, vector lengths are adjusted to the maximum length within the batch.
) -> List<Struct<
  PassThrough:* -- passed to the PassThrough argument without change,
  *:* -- other fields correspond to the data type specified in the third argument
>> -- result
```

`TensorFlow::RunBatch` is a fairly complex polymorphic function designed to be executed in [PROCESS](../../syntax/process.md). Therefore, we strongly recommend that you review the example below.

## Usage example

**Task:** given email address, guess the gender of it's owner using the TensorFlow and pre-trained LSTM neural network.

{% cut "Input table `emails_sample` data example" %}

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
PRAGMA yt.DataSizePerJob = "128M"; -- as this computation is quite CPU intensive,
                                   -- it's a good idea to request smaller
                                   -- data size per job
PRAGMA file("email.pb", "<location of serialized graph>");
$session = TensorFlow::InitSession(
    FilePath("email.pb") -- path to TensorFlow graph definition
);

PRAGMA library("char_table.sql"); -- treat attached file as source for imports
IMPORT char_table SYMBOLS $char_to_float; -- import lambda from library that converts chars of emails
                                          -- the same way as it has been done during model training

-- Lambda function to preprocess emails:
$email_to_float_vector = ($email) -> {
    $cut = FIND($email, "@"); -- keep only the login
    $cut = MIN_OF($cut, 20u) ?? 20u;
    $email = SUBSTRING($email, 0, $cut); -- cut long logins too
    $email_bytes = String::ToByteList($email); -- String to list of bytes
    RETURN ListFlatMap($email_bytes, $char_to_float); -- apply lambda
};

$input = (
    SELECT
        $email_to_float_vector(email) AS input_1, -- the name of column
                                                  -- should match the name
                                                  -- of TensorFlow graph
                                                  -- input
        TableRow() AS PassThrough -- special column name to pass
                                  -- any data through the TensorFlow
                                  -- batch mode, in this example
                                  -- the whole input row
    FROM emails_sample);
--------------------------------------------------------------------------------
-- Conventions on border between TensorFlow and YQL:
-- - While name of input column should match the name of graph input,
--   it's type should match the tensor shape and data type.
-- - Supported data types: Float, Double, Int32, Int64, Uint8, Bool
-- - List<...> means vector, while just data type - scalar,
--   matrices and other shapes are not supported yet.
-- - This model has only one input, but there might be many.
-- - As you'll see below, the same rules apply to graph output.
-- - PassThrough column is currently required, but may become optional
--   in the future.
--------------------------------------------------------------------------------

$processed = (PROCESS $input
USING TensorFlow::RunBatch( -- Apply TensorFlow graph in batches
    $session,
    TableRows(), -- Special name for the iterator over table rows
    -- Description of graph output to compute:
    Struct<network_output:List<Float>>,
    -- - Each struct member represents one graph output.
    -- - Name should match the one used during training.
    -- - Mapping between tensors and YQL types is the same
    --   as with input.
    ListCreate(String), -- TargetNodeNames
    128,                  -- BatchSize
    20                    -- MinVectorSize
));

SELECT
    network_output[0] ?? 0.0f AS score, -- the result of gender guess given email:
                                        -- - close to 0.0 means female
                                        -- - close to 1.0 means male
                                        -- - close to 0.5 means "not sure"
    PassThrough.email AS email, -- the original email
    PassThrough.yandexuid AS yandexuid -- other column from input
FROM $processed;
```
