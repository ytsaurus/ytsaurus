# Конфигурационные параметры для запуска Spark задач

В данном разделе приведён список конфигурационных параметров, которые можно передавать при запуске задач на Spark. Для этого используется стандартный способ указания дополнительных параметров через опцию `--conf` основных Spark команд, таких как `spark-submit`, `spark-shell`, а также {{product-name}}-обёрток над ними `spark-submit-yt` и `spark-shell-yt`.

## Основные опции { #main }

Большинство опций доступны начиная с версии 1.23.0, если не указано иное.

| **Параметр** | **Значение по умолчанию** | **Описание** | **С какой версии** |
| ------------ | ------------------------- | ------------ | ------------------ |
| `spark.yt.write.batchSize` | `500000` | Размер данных, отправляемых через одну операцию `WriteTable` | - |
| `spark.yt.write.miniBatchSize` | `1000` | Размер блока данных, отправляемого в `WriteTable` | - |
| `spark.yt.write.timeout` | `120 seconds` | Ограничение на ожидание записи одного блока данных | - |
| `spark.yt.write.typeV3.enabled` (`spark.yt.write.writingTypeV3.enabled` до 1.75.2) | `true` | Запись таблиц со схемой в формате [type_v3](../../../../../user-guide/storage/data-types.md) вместо `type_v1` | 1.75.3 |
| `spark.yt.read.vectorized.capacity` | `1000` | Максимальное количество строк в батче при чтении через `wire` протокол | - |
| `spark.yt.read.arrow.enabled` | `true` | Использовать `arrow` формат для чтения данных (если это возможно) | - |
| `spark.hadoop.yt.timeout` | `300 seconds` | Таймаут на чтение из {{product-name}} | - |
| `spark.yt.read.typeV3.enabled` (`spark.yt.read.parsingTypeV3.enabled` до 1.75.2) | `true` | Чтение таблиц со схемой в формате [type_v3](../../../../../user-guide/storage/data-types.md) вместо `type_v1` | 1.75.3 |
| `spark.yt.read.keyColumnsFilterPushdown.enabled` | `true` | Использовать фильтры Spark-запроса для выборочного чтения из {{product-name}} | - |
| `spark.yt.read.keyColumnsFilterPushdown.union.enabled` | `false` | Объединять все фильтры в непрерывный диапазон при выборочном чтении | - |
| `spark.yt.read.keyColumnsFilterPushdown.ytPathCount.limit` | `100` | Максимальное количество диапазонов таблицы при выборочном чтении | - |
| `spark.yt.transaction.timeout` | `5 minutes` | Таймаут на транзакцию записывающей операции | - |
| `spark.yt.transaction.pingInterval` | `30 seconds` | Периодичность пингования транзакции записывающей операции | - |
| `spark.yt.globalTransaction.enabled` | `false` | Использовать [глобальную транзакцию](../../../../../user-guide/data-processing/spyt/read-transaction.md) | - |
| `spark.yt.globalTransaction.id` | `None` | Идентификатор глобальной транзакции | - |
| `spark.yt.globalTransaction.timeout` | `5 minutes` | Таймаут глобальной транзакции | - |
| `spark.hadoop.yt.user` | - | Имя пользователя {{product-name}} | - |
| `spark.hadoop.yt.token` | - | Токен пользователя {{product-name}} | - |
| `spark.yt.read.ytPartitioning.enabled` | `true` | Использовать партиционирование таблиц средствами {{product-name}} | 1.72.0 |
| `spark.yt.read.planOptimization.enabled` | `false` | Оптимизировать агрегации и джойны на сортированных входных данных | - |
| `spark.yt.read.keyPartitioningSortedTables.enabled` | `true` | Использовать партиционирование по ключам для сортированных таблиц, необходимо для оптимизации планов | - |
| `spark.yt.read.keyPartitioningSortedTables.unionLimit` | `1` | Максимальное количество объединений партиций при переходе от чтения по индексам к чтению по ключам | - |
| `spark.yt.read.transactional` | `true` | Использовать snapshot lock для чтения в случае отсутствия транзакции. Рекомендуется отключать эту опцию в случае чтения неизменяемых данных для повышения производительности чтения. | 2.6.0 |
| `spark.yt.read.ytDistributedReading.enabled` | `false` | Использовать распределенное API для чтения данных из {{product-name}}. Данный способ позволяет уменьшить количество обращений к мастеру {{product-name}} при чтении, но пока не совместим с опцией `spark.yt.read.planOptimization.enabled` | 2.8.0 |
| `spark.yt.write.distributed.enabled` | `false` | Использовать распределенное API для записи данных в {{product-name}}. Данный способ позволяет уменьшить количество обращений к мастеру {{product-name}} при записи, но применим только к работе со статическими таблицами | 2.8.0 |

## Опции для запуска задач напрямую { #direct-submit }

| **Параметр** | **Значение по умолчанию** | **Описание** | **С какой версии** |
| ------------ | ------------------------- | ------------ | ------------------ |
| `spark.ytsaurus.config.global.path` | `//home/spark/conf/global` | Путь к документу с глобальной конфигурацией Spark и SPYT на кластере | 1.76.0 |
| `spark.ytsaurus.config.releases.path` | `//home/spark/conf/releases` для релизных версий, `//home/spark/conf/pre-releases` для предварительных версий | Путь к конфигурации релиза SPYT | 1.76.0 |
| `spark.ytsaurus.distributives.path` | `//home/spark/distrib` | Путь к директории с дистрибутивами Spark. Внутри этой директории структура имеет вид `a/b/c/spark-a.b.c-bin-hadoop3.tgz` | 2.0.0 |
| `spark.ytsaurus.config.launch.file` | `spark-launch-conf` | Название документа с конфигурацией релиза, который лежит внутри директории `spark.ytsaurus.config.releases.path` | 1.76.0 |
| `spark.ytsaurus.spyt.version` | Совпадает с версией SPYT на клиенте | Версия SPYT, которую нужно использовать на кластере при запуске Spark приложения. | 1.76.0 |
| `spark.ytsaurus.driver.maxFailures` | 5 | Максимально допустимое количество падений драйвера перед тем, как операция будет считаться неуспешной | 1.76.0 |
| `spark.ytsaurus.executor.maxFailures` | 10 | Максимально допустимое количество падений экзекьютора перед тем, как операция будет считаться неуспешной | 1.76.0 |
| `spark.ytsaurus.executor.operation.shutdown.delay` | 10000 | Максимально допустимое время в миллисекундах для ожидания завершения экзекьюторов при остановке приложения перед принудительной остановкой операции с экзекьюторами | 1.76.0 |
| `spark.ytsaurus.pool` | - | Пул планировщика, в котором необходимо запускать операции драйвера и экзекьютора | 1.78.0 |
| `spark.ytsaurus.python.binary.entry.point` | - | Функция, используемая в качестве точки входа при использовании скомпилированных Python задач | 2.4.0 |
| `spark.ytsaurus.python.executable` | - | Путь к интерпретатору python, используемый в драйвере и экзекьюторах | 1.78.0 |
| `spark.ytsaurus.tcp.proxy.enabled` | false | Используется ли TCP прокси для доступа к операции | 2.1.0 |
| `spark.ytsaurus.tcp.proxy.range.start` | 30000 | Минимальный номер порта для TCP прокси | 2.1.0 |
| `spark.ytsaurus.tcp.proxy.range.size` | 1000 | Размер диапазона портов, которые могут быть выделены для TCP прокси | 2.1.0 |
| `spark.ytsaurus.cuda.version` | - | Версия CUDA, используемая для Spark приложений. Имеет смысл, если расчеты используют GPU | 2.1.0 |
| `spark.ytsaurus.redirect.stdout.to.stderr` | false | Перенаправление вывода пользовательских скриптов из stdout в stderr | 2.1.0 |
| `spark.ytsaurus.remote.temp.files.directory` | `//tmp/yt_wrapper/file_storage` | Путь к кешу на кипарисе для загрузки локальных скриптов | 2.4.0 |
| `spark.ytsaurus.annotations` | - | Аннотации для операций с драйвером и экзекьюторами | 2.2.0 |
| `spark.ytsaurus.driver.annotations` | - | Аннотации для операции с драйвером | 2.2.0 |
| `spark.ytsaurus.executors.annotations` | - | Аннотации для операции с экзекьюторами | 2.2.0 |
| `spark.ytsaurus.driver.watch` | true | Флаг для выполнения мониторинга операции с драйвером при выполнении в кластерном режиме | 2.4.2 |
| `spark.ytsaurus.network.project` | - | Название сетевого проекта, в котором запускается Spark приложения | 2.4.3 |
| `spark.hadoop.yt.mtn.enabled` | false | Флаг включения поддержки MTN | 2.4.3 |
| `spark.ytsaurus.squashfs.enabled` | false | Использование squashFS слоёв в {{product-name}} джобе вместо porto слоёв | 2.6.0 |
| `spark.ytsaurus.client.rpc.timeout` | - | Таймаут, используемый в rpc клиенте для старта {{product-name}} операций | 2.6.0 |
| `spark.ytsaurus.rpc.job.proxy.enabled` | true | Флаг использования rpc прокси, встроенной в job прокси | 2.6.0 |
| `spark.ytsaurus.java.home` | `/opt/jdk[11,17]` | Путь к домашней директории JDK, используемой в контейнерах кластера. Зависит от используемой JDK на клиентской стороне. Допустимы JDK11 и JDK17. | 2.6.0 |
| `spark.ytsaurus.shuffle.enabled` | false | Использовать [{{product-name}} Shuffle сервис](../../../../../user-guide/data-processing/spyt/shuffle.md) | 2.7.2 |
| `spark.ytsaurus.executor.state.poll.interval` | 20s | Период проверки состояния операции с экзекьюторами. Если эта операция находится в финальном статусе то драйвер будет остановлен | 2.8.0 |


## Опции для конфигурации {{product-name}} Shuffle сервиса { #shuffle }

| **Параметр** | **Значение по умолчанию** | **Описание** | **С какой версии** |
| ------------ | ------------------------- | ------------ | ------------------ |
| `spark.ytsaurus.shuffle.transaction.timeout` | 5m | Таймаут транзакции, под которой записываются shuffle чанки. При штатной работе транзакция периодически пингуется драйвером, таймаут задаёт время между последним пингом и откатом транзакции с удалением чанков | 2.7.0 |
| `spark.ytsaurus.shuffle.account` | intermediate | [Аккаунт](../../../../../user-guide/storage/accounts.md), используемый для записи shuffle чанков | 2.7.0 |
| `spark.ytsaurus.shuffle.medium` | - | [Медиум](../../../../../user-guide/storage/media.md), используемый для записи shuffle чанков. По умолчанию равен установленному в системе | 2.7.0 |
| `spark.ytsaurus.shuffle.replication.factor` | - | Коэффициент репликации shuffle чанков. По умолчанию равен установленному в системе | 2.7.0 |
| `spark.ytsaurus.shuffle.partition.column` | partition | Название колонки в чанке, используемой для хранения номера целевой партиции | 2.7.0 |
| `spark.ytsaurus.shuffle.write.row.size` | 8m | Максимальный размер одной строки в чанке c shuffle данными. Данное значение не связано с размером непосредственно строк shuffle данных, это значение служит для разделения сериализованных shuffle данных по строкам чанка. Уменьшение этого значения приводит к большему количеству строк в чанке, при увеличении можно превысить максимально допустимый размер строки чанка | 2.7.0 |
| `spark.ytsaurus.shuffle.write.buffer.size` | 10 | Размер буфера записи (в количестве строк) shuffle данных в  {{product-name}}. Этот параметр необходимо выставлять вместе с `spark.ytsaurus.shuffle.write.row.size` для избежания переполнения оперативной памяти | 2.7.0 |
| `spark.ytsaurus.shuffle.write.config` | - | Дополнительные параметры записи shuffle данных в {{product-name}} в [YSON](../../../../../user-guide/storage/yson.md) формате. Соответствует конфигурации [TableWriter](../../../../../user-guide/storage/io-configuration.md#table_writer) | 2.7.0 |
| `spark.ytsaurus.shuffle.read.config` | - | Дополнительные параметры чтения shuffle данных в {{product-name}} в [YSON](../../../../../user-guide/storage/yson.md) формате. Соответствует конфигурации [TableReader](../../../../../user-guide/storage/io-configuration.md#table_reader) | 2.7.0 |


## Опции для конфигурации Spark Connect сервера { #spyt-connect }

| **Параметр** | **Значение по умолчанию** | **Описание** | **С какой версии** |
| ------------ | ------------------------- | ------------ | ------------------ |
| `spark.ytsaurus.connect.idle.timeout` | 10m | Время ожидания после завершения выполнения последнего запроса до остановки сервера Spark Connect | 2.8.0 |
| `spark.ytsaurus.connect.token.refresh.period` | - | Периодичность пингования временного токена, используемого для выполнения запросов через Spark Connect. Используется только в связке с Query Tracker | 2.8.0 |

## Опции для запуска задач во внутреннем кластере { #spark-submit-yt-conf }

Для запуска задач во внутреннем кластере используется обёртка `spark-submit-yt`. Её параметры соответствуют параметрам команды `spark-submit` из дистрибутива Spark, за следующим исключением:

- Вместо параметра `--master` используются параметры `--proxy` и `--discovery-path`. Они определяют, какой {{product-name}} кластер будет использоваться для запуска вычислений, и в какой внутренний Spark кластер на этом {{product-name}} кластере будет отправлена задача, соответственно.
