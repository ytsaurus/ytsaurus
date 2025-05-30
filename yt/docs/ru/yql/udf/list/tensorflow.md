# TensorFlow UDF
Применение [TensorFlow](https://www.tensorflow.org) графов вычислений.
``` yql
TensorFlow::InitSession(
  String, -- путь к сериализованному в protobuf формате графу вычислений, единственный обязательный аргумент
  Bool?, -- EnableMKLDNN, включение MKLDNN https://01.org/mkl-dnn
  Bool?, -- MKLCBWRCompatible, включение совместимости с SSE2 для MKL https://software.intel.com/en-us/mkl
  Bool?, -- MKLDNNUseWeightsAsGiven, по умолчанию false
  Bool?, -- AllowSoftPlacement, по умолчанию true
  Int32?, -- IntraOpParallelismThreads, по умолчанию 0
  Int32?, -- InterOpParallelismThreads, по умолчанию 0
) -> Resource<TensorFlow.Session> -- указатель на TensorFlow сессию с проинициализированным графом вычислений

TensorFlow::RunBatch(
  Resource<TensorFlow.Session>, -- результат TensorFlow::InitSession
  List<Struct<
    PassThrough:* -- произвольные данные, которые пропускаются через функцию без изменений
    *:* -- имена остальных полей соответствуют именам входов графа вычислений, которые будут переданы в TensorFlow,
        -- а их типы данных — соответствующим формам тензоров; примитивный тип данных соответствует скалярному
        -- тензору, список - вектору, матрицы на данный момент не поддерживаются.
  >>,
  Type<Struct<*:*>>, -- описание выходов графа, которые необходимо вычислить по аналогии со входами
  List<String>?, -- TargetNodeNames, опциональный список имен узлов графа, которые нужно вычислить, но не возвращать
  Uint32?, -- BatchSize, по умолчанию 128
  Uint32?, -- MinVectorSize, по умолчанию 1, позволяет выроврять входные вектора до указанной длины,
           -- если на входе они имеют разную длину <= указанной.
           -- Если не выставлен, длины векторов батча выравниваются до максимальной в рамках батча.
) -> List<Struct<
  PassThrough:* -- переданное в аргумент PassThrough без изменений,
  *:* -- остальные поля соответствуют типу данных, указанному в третьем аргументе
>> -- результат
```

`TensorFlow::RunBatch` является достаточно сложной полиморфной функцией, предназначенной для выполнения в [PROCESS](../../syntax/process.md), так что настоятельно рекомендуем ознакомиться с примерами:

* [Пример в tutorial]({{yql.link}}/Tutorial/yt_25_TensorFlow)
* [Дополнительные примеры]({{source-root}}/yql/udfs/ml/tensorflow/test/cases/)
