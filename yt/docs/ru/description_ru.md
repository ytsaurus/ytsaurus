annotation: |-
  # Справочник конфигураций

  Данный файл содержит описание всех спек и конфигов, используемых для конфигурации {{product-name}} Flow. Алгоритм генерации этого файла рекурсивно ищет все подконфиги. Он несовершенен.{% if audience == "internal" %} Если чего-то нет, вы можете обратиться в чат [YT Flow Public](https://nda.ya.ru/t/hcJkQdBD7LNa9V), и мы постараемся это добавить.{% endif %}

internal_yson_structs:
  - "NYT::NFlow::TUnitedParameters<NYT::NFlow::TAtLeastOnceLogbrokerFramingSink>"
  - "NYT::NFlow::TUnitedParameters<NYT::NFlow::TAtLeastOnceLogbrokerSink>"
  - "NYT::NFlow::TUnitedParameters<NYT::NFlow::TLogbrokerFramingSink>"
  - "NYT::NFlow::TUnitedParameters<NYT::NFlow::TLogbrokerFramingSource>"
  - "NYT::NFlow::TUnitedParameters<NYT::NFlow::TLogbrokerMultiLineSource>"
  - "NYT::NFlow::TUnitedParameters<NYT::NFlow::TLogbrokerSink>"
  - "NYT::NFlow::TUnitedParameters<NYT::NFlow::TLogbrokerSource>"
  - "NYT::NFlow::TUnitedParameters<NYT::NFlow::TMoniumSink>"
  - "NYT::NFlow::TUnitedParameters<NYT::NFlow::TMoniumSource>"

no_main_fields_statement: |-
  **В структуре нет основных параметров.**

fields_naming:
  description: >-
    **Описание**
  default_value: >-
    **Значение по умолчанию**
  type: >-
    **Тип**
  field: >-
    **Параметр**
  source: >-
    Источник
  required_argument: >-
    **Обязательный параметр**
  possible_values: >-
    Возможные значения
  advanced_parameters: >-
    **Дополнительные параметры**


simple_types:
  "TDuration":
    description: |-
      Тип продолжительности временного промежутка. Он может парситься из yson-значений следующих типов:
      * uint64. Задаёт время в **миллисекундах**.
      * int64. Так же как и uint64. Выбрасывается исключение в случае отрицательного значения.
      * double. Так же как и int64. Поведение в случае значения выходящего за пределы значений uint64 не определено.
      * string. Задаёт время в форматах: `10s`, `15ms`, `15.05s`, `20us`, `25`. Если суффикс не указан, то число трактуется как время в **секундах**.


  "NYT::NYTree::TSize":
    description: |-
      Тип-обёртка над int64, расширяющая его парсинг из yson. Он может парситься из yson-значений следующих типов:
      * int64. В этом случае значение берётся как есть.
      * uint64. Значение берётся как есть, но если оно не входит в диапазон допустимых значений int64, то выбрасывается исключение.
      * string. Значение парсится из строки с поддержкой суффиксов, определяющих множитель.
          * `K`, `M`, `G`, `T`, `P`, `E` &mdash; суффиксы, соответствующие множителям $1000$, $1000^2$ и т. д.
          * `Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei` &mdash; суффиксы, соответствующие множителям $2^{10}$, $2^{20}$ и т. д.

          То есть `12M` &mdash; это $12~000~000$, `1Ki` &mdash; это $1024$.


yson_structs:
  "NYT::NFlow::TPipelineSpec":
    members:
      "computations":
        description: >-
          Именованное перечисление всех узлов пайплайна.
      "streams":
        description: >-
          Именованное перечисление всех потоков данных.
      "resources":
        description: >-
          Именованное перечисление всех ресурсов.


  "NYT::NFlow::TComputationSpec":
    members:
      "computation_class_name":
        description: |-
          Имя класса Computation.

          Класс должен быть зарегистрирован с помощью макроса `YT_FLOW_DEFINE_COMPUTATION`
      "parameters":
        description: >-
          Произвольные параметры для `Computation` и `ComputationController`
      "group_by_schema":
        description: |-
          Схема для группировки всех входных потоков, является схемой динтаблицы. Свойства и требования к схеме:

          * Не содержит никаких правил на порядок.
          * Может содержать вычислимые поля.
          * Должна быть пустой только у `SourceComputation`.
          * Для остальных `Computation` первая колонка должна иметь тип `uint64`. Рекомендуется, чтобы эта колонка была вычислимым хешем от остальных колонок (например, `farm_hash`). Нужно иметь ввиду, что использование операции взятия остатка при целочисленном делении `%` и `bigb_hash` в `expression` запрещено.
      "input_stream_ids":
        description: |-
          Входные потоки.

          Схемы всех потоков должны быть приводимы к `group_by_schema`.
      "output_stream_ids":
        description: >-
          Выходные потоки Computation'а. В частности, могут использоваться как входы у других Computation'ов.
      "streams_dependency":
        description: |-
          Описание зависимостей между потоками, включая внутренние `source` и `timer` потоки.

          Пример: `{stream1, {stream2, stream3}}` &mdash; `stream1` зависит от потоков `stream2` и `stream3`.

          Указывается только для `timer`- и `output`-потоков. В случае отсутствия описания для потока список зависимостей будет сгенерирован автоматически:

          * у `timer`-потоков в списке будут все `input`- и `source`-потоки;
          * у `output`-потоков &mdash; все `timer`, `input` и `source`.
      "watermark_strategy":
        description: |-
          Настройки, связанные с `EventWatermark`.

          Необязательный параметр. Должен быть указан в рамках `SourceComputation`.
      "required_resource_ids":
        description: |-
          Список необходимых ресурсов для работы `Computation`.

          Имеет параметр `alias` для локального переименования ресурса (например, `Computation` ожидает ресурс с именем `YTClient`, а подаваемый ресурс имеет другое имя). Через параметры `controller` и `worker` можно управлять созданием ресурса на `Controller` и на `Worker`.
      "timer_streams":
        description: >-
          Настройки всех `timer` потоков.
      "source_streams":
        description: >-
          Настройки всех `source` потоков.
      "sinks":
        description: >-
          Настройки всех `sink`.
      "heavy_hitters":
        description: >-
          Настройки обнаружения высокочастотных ключей.
      "input_ordering":
        description: >-
          Настройки порядка обработки входных потоков.
      "distribution_ordering":
        description: >-
          Настройки порядка распределения выходных сообщений. Управляет тем, может ли быть гонка между останавливающимися и работающими партициями с точки зрения порядка распределения выходных сообщений порожденных из входящих сообщений с одним и тем же ключом.
      "external_state_managers":
        description: |-
          Декларативное объявление [external state](../cpp/state.md#external-state) менеджеров для этого `Computation`. Ключ — имя клиента, на которое подписывается `Computation` через `IJobInitContext::InitExternalStateClient` (должен начинаться с `/`, например `/state`); значение содержит имя класса менеджера и его параметры.
      "external_state_joiners":
        description: |-
          Декларативное объявление [external state](../cpp/state.md#external-state-joiner) joiner'ов (read-only-доступ к внешним стейтам через join по ключу) для этого `Computation`. Ключ — имя клиента, на которое подписывается `Computation` через `IJobInitContext::InitExternalStateClient` (должен начинаться с `/`, например `/state`); значение содержит имя класса joiner'а и его параметры.
      "state_joiners":
        description: |-
          Декларативное объявление [state](../cpp/state.md#state-joiner) joiner'ов (read-only-доступ к внутреннему стейту другого `Computation` через join по ключу) для этого `Computation`. Ключ — имя клиента, на которое подписывается `Computation` через `IJobInitContext::InitClient` (должен начинаться с `/`); значение указывает целевой `computation_id`, его `state_name` и `join_on`.
      "use_compact_input_messages":
        description: |-
          Использовать компактную таблицу `compact_input_messages` для [дедупликации входных сообщений](../concepts/guarantees.md) вместо `input_messages`. Компактный ключ дедупликации хранит только хеш `key[0]` и `CityHash128(message_id)`, что снижает размер строки (~200 байт → ~71 байт).

          Необязательный параметр. Если значение не задано, таблица выбирается автоматически: компактная используется всегда, кроме случая, когда включён `experimental_enable_non_uint_key` (тогда первая колонка ключа может быть не `uint64`, и для дедупликации нужен полный ключ). Явно заданное значение имеет приоритет над автоматическим выбором. Включать `use_compact_input_messages` вместе с `experimental_enable_non_uint_key` запрещено.


  "NYT::NFlow::TWatermarkStrategySpec":
    members:
      "event_timestamp_assigner":
        description: >-
          Автоматическое заполнение `EventTimestamp` всех `output` сообщений на базе значения определённой колонки.
      "watermark_generator":
        description: >-
          Настройки алгоритма генерации `EventWatermark`. При отсутствии - `EventWatermark` будет равен `MaxEventTimestamp`
      "watermark_alignment":
        description: >-
          Настройки выравнивания `source` между разными партициями, в том числе разных `Computation`
      "watermark_percentile":
        description: >-
          Настройка "персентильного" `EventWatermark`. Используется для игнорирования самых старых событий в пределах сконфигурированного процента.


  "NYT::NFlow::TEventTimestampAssignerSpec":
    members:
      "column":
        description: >-
          Колонка со временем
      "format":
        description: >-
          Используемый формат времени (`seconds` и `milliseconds`)
      "limit_by_system_timestamp":
        description: >-
          Ограничить `EventTimestamp` значением `SystemTimestamp` для защиты от битых значений из будущего


  "NYT::NFlow::TWatermarkGeneratorSpec":
    members:
      "use_source_watermark":
        description: >-
          Использовать `SourceWatermark` для генерации `EventWatermark`. Эта опция полностью отключает все эвристики для определения `EventWatermark` и её нужно использовать строго в тех случаях, когда во входном потоке есть специальные маркеры с `EventWatermark`.
      "out_of_orderness_bound":
        description: >-
          Ограничение сверху на возможное переупорядочивание событий, используемое для оценки `EventWatermark`.
      "idle_partitions":
        description: >-
          Настройки эвристики для игнорирования партиций с нулевым потоком записи.
      "unavailable_partition_groups":
        internal: true
        description: >-
          Настройки эвристики для игнорирования недоступных групп партиций (например, партиций в отключённом датацентре основной инсталляции `Logbroker`)


  "NYT::NFlow::TIdlePartitionsSpec":
    members:
      "duration":
        description: >-
          Время, как долго партиция должна быть пустой, чтобы поверить, что в неё не идёт записи.
      "max_ratio":
        description: >-
          Доля партиций, которые могут быть исключены из расчёта вотермарка этой логикой. Идейный смысл опции &mdash; защита от продвижения вотермарка, когда поставщик данных прекратил писать сообщения в очередь по причине инцидента на его стороне. Выставление значения опции в `1.0` по сути отключает эту защиту.


  "NYT::NFlow::TUnavailablePartitionGroupsSpec":
    members:
      "max_unavailable_groups":
        internal: true
        description: >-
          Сколько групп доступности партиций могут быть недоступны, чтобы вотермарк всё ещё продвигался (их вотермарк при этом прячется). Например, при чтении основной инсталляции `Logbroker` из трёх ДЦ значение `1` позволяет продолжать продвижение вотермарка при отключении одного ДЦ.
      "min_available_groups":
        internal: true
        description: >-
          Сколько групп доступности партиций должны быть доступны, чтобы вотермарк продвигался. Защищает источники с одной группой (`lbkx` и не-`Logbroker`): при недоступности единственной группы вотермарк не продвигается, чтобы не потерять проджойн.


  "NYT::NFlow::TWatermarkAlignmentSpec":
    members:
      "group_name":
        description: >-
          Имя группы
      "drift_bound":
        description: >-
          Максимальное возможное опережение вотермарка отдельной партиции от вотермарка всей группы
      "read_delays":
        description: >-
          Опция для продвинутого использования. Если `EventTimestamp` выходного сообщения больше, чем `EventWatermark - Delay` хотя бы для одного указанного потока, то чтение приостанавливается. При неправильной настройке опция может привести к полной остановке чтения.


  "NYT::NFlow::TWatermarkPercentileSpec":
    members:
      "value":
        description: >-
          Процент безусловно учитываемых для расчёта `EventWatermark` событий
      "delay":
        description: >-
          Порог свежести для событий ниже выбранного персентиля


  "NYT::NFlow::TTimerSpec":
    members:
      "time_type":
        description: >-
          Используемый таймером тип времени: `event_time`, `system_time`, `real_time`
      "streams":
        description: >-
          Перечисление потоков, за которыми таймер следит.
      "streams_with_delays":
        description: >-
          Перечисление потоков, за которыми таймер следит с указанием индивидуальной задержки
      "deduplicate_equal_timestamps":
        description: >-
          Включение логики дедупликации таймеров: при попытке создания таймера с тем же ключом и `trigger_timestamp`, останется только один, с меньшим значением `EventTimestamp`


  "NYT::NFlow::TSourceSpec":
    members:
      "source_class_name":
        description: |-
          Имя соответствующего класса.

          Должен быть зарегистрирован с помощью `YT_FLOW_DEFINE_SOURCE`.
      "parameters":
        description: >-
          Произвольные параметры для соответствующего класса.


  "NYT::NFlow::TSinkSpec":
    members:
      "sink_class_name":
        description: |-
          Имя соответствующего класса.

          Должен быть зарегистрирован с помощью `YT_FLOW_DEFINE_SINK`.
      "input_stream_ids":
        description: |-
          Список входных потоков.

          Здесь могут быть только потоки, указанные в `output_stream_ids` всего `Computation`.
      "parameters":
        description: >-
          Произвольные параметры для соответствующего класса.


  "NYT::NFlow::THeavyHittersSpec":
    members:
      "threshold":
        description: >-
          Ключи, которые встречаются в потоке чаще, чем `threshold`, будут считаться высокочастотными. Алгоритм использует `O(1 / threshold)` памяти.
      "limit":
        description: >-
          Максимальное количество показываемых высокочастотных ключей.
      "window":
        description: >-
          Временное окно.


  "NYT::NFlow::TInputOrderingSpec":
    members:
      "time_type":
        description: >-
          Время для сортировки: `event_time`, `system_time`, `real_time`
      "stream_delays":
        description: >-
          Задержки для входных потоков. Если поток не указан &mdash; то задержки не будет


  "NYT::NFlow::TStreamSpec":
    members:
      "schema":
        description: |-
          Cхема потока. Является схемой динтаблицы. Не может содержать сортировки и вычислимые поля.

          Изменение схем рекомендуется осуществлять через полную остановку пайплайна: в этом случае внутри пайплайна не останется `output` сообщений.

  "NYT::NFlow::TResourceSpec":
    members:
      "resource_class_name":
        description: |-
          Имя соответствующего класса.

          Должен быть зарегистрирован с помощью макроса `YT_FLOW_DEFINE_RESOURCE`.
      "parameters":
        description: >-
          Произвольные параметры для соответствующего класса.
      "dependencies":
        description:  >-
          Ресурсы, от которых зависит данный ресурс. Повторяет структуру `required_resource_ids` из `Computation`.
      "always_on":
        description: >-
          Если `true`, ресурс загружается заранее при старте юнита и держится в памяти всё время
          жизни юнита (не выгружается), а не лениво при первом обращении. Загружается только на тех
          юнитах, где ресурс требует хотя бы одна computation (см. `worker` / `controller` в
          `required_resource_ids`): ресурс, нужный только на worker'е, не грузится на controller'е,
          а ресурс, который не требует ни одна computation, не грузится вовсе. Взаимоисключающе с
          `preload_required`.


  "NYT::NFlow::TDynamicPipelineSpec":
    members:
      "target_state":
          description: >-
            Целевое состояние системы. Для изменения используйте команды `start-pipeline`, `stop-pipeline`, `pause-pipeline`.
      "computations":
          description: >-
            Именованное перечисление всех узлов пайплайна.
      "job_manager":
          description: >-
            Настройки `JobManager`.
      "message_distributor":
        description: >-
          Настройки `MessageDistributor`.
      "job_tracker":
        description: >-
          Настройки `JobTracker`.
      "controller_connector":
        description: >-
          Настройки `ControllerConnector`.
      "throttlers":
        description: >-
          Набор именованных распределённых троттлеров, общих для всех `Computation` пайплайна. Ключ &mdash; id троттлера. Подробнее в разделе [Distributed Throttler](../concepts/distributed_throttler.md).


  "NYT::NFlow::TDynamicMessageDistributorSpec":
    members:
      "max_processed_batch_size":
        advanced: true
        description: >-
          Максимальное число завершённых сообщений, о которых воркер сообщает в одном ответе `PushMessages`.


  "NYT::NFlow::TDynamicComputationSpec":
    members:
      "draining":
        description: |-
          Режим `Draining` нужен для сброса пайплайна &mdash; в нём `Computation` перестанет запускать таймеры и доставать новые события из `Source`.

          Команда `stop-pipeline` останавливает весь пайплайн через полный сброс. В отдельных `Computation` режим можно выставлять для дебага.
      "empty_batch_backoff":
        description: >-
          В случае пустой эпохи `Computation` дополнительно «спит» указанное время. Не рекомендуется выставлять параметр меньше 100 мс.
      "batch_duration":
        description: >-
          Время максимального набора входного батча.
      "max_rows_per_batch":
        description: >-
          Максимальный размер входного батча в строках. Считается отдельно для `input_streams`, `timer_streams` и для каждого из `source_streams`.
      "max_bytes_per_batch":
        description: >-
          Максимальный размер входного батча в байтах. Считается аналогично `max_rows_per_batch`.
      "blocked_time_window":
        description: >-
          Окно усреднения для доли времени, которую джоба провела заблокированной на каждом из лимитов
          (`blocked_share` в диагностиках). Доля нормируется на время жизни джобы, поэтому джоба,
          заблокированная с самого старта, показывает `~1` независимо от своего возраста.
      "parameters":
        description: >-
          Произвольные динамические параметры класса `Computation`.
      "source_streams":
        description:  >-
          Динамические параметры всех `Sources`.
      "sinks":
        description: >-
          Динамические параметры всех `Sinks`.
      "input_rows_throttler_id":
        description: >-
          Id троттлера для ограничения скорости обработки сообщений. Если задан, перед каждой итерацией `Computation` ждёт квоту, равную числу сообщений во входном батче. Id должен присутствовать в `dynamic_spec/throttlers`. Подробнее в разделе [Distributed Throttler](../concepts/distributed_throttler.md).
      "input_bytes_throttler_id":
        description: >-
          Аналогично `input_rows_throttler_id`, но квотируется суммарный `byte_size` сообщений батча &mdash; системный размер их сериализованного представления.
      "skip_if_expression":
        description: |-
          YTQL-предикат, по которому входные сообщения отфильтровываются (пропускаются) ещё до обработки `Computation`. Сообщение пропускается (дропается), если предикат вычисляется в булево `true`.

          Предикат вычисляется над колонками `payload` сообщения плюс мета-колонками `$message_id`, `$stream_id`, `$system_timestamp`, `$event_timestamp`, `$alignment_timestamp`. Результат обязан быть булевым: `NULL` или небулевой результат приводит к исключению.

          Применяется как к `input`-, так и к `source`-потокам. Число пропущенных сообщений экспортируется метриками `input_streams/skipped_by_expression_count` и `source_streams/skipped_by_expression_count`.


  "NYT::NFlow::TDynamicThrottlerSpec":
    description: >-
      Конфигурация именованного распределённого троттлера.
    members:
      "limit":
        description: >-
          Средняя скорость выдачи квоты в единицах в секунду. `none` означает `unlimited`.
      "period":
        description: >-
          Период усреднения средней скорости в алгоритме [token bucket](https://en.wikipedia.org/wiki/Token_bucket): за `period` бакет пополняется до объёма `limit * period` единиц. Этот же объём &mdash; максимально возможный burst.
      "request_period":
        description: >-
          Целевой интервал между походами клиента на контроллер (размер prefetch подстраивается под него).
      "retrying_channel":
        description: >-
          Параметры ретраев запросов к сервису троттлера. Дефолт рассчитан на то, чтобы пережить смену контроллера-лидера даже если она затянется.
      "rpc_timeout":
        description: >-
          Таймаут одного запроса `RequestQuota`.


  "NYT::NFlow::TDynamicSourceSpec":
    members:
      "parameters":
        description: >-
          Произвольные динамические параметры соответствующего класса `Source`.


  "NYT::NFlow::TDynamicSinkSpec":
    members:
      "parameters":
        description: >-
          Произвольные динамические параметры соответствующего класса `Sink`.


  "NYT::NFlow::TDynamicJobManagerGroupSpec":
    members:
      "minimum_worker_count":
        description: |-
          Минимальное число воркеров, на которые стоит распределять нагрузку.

          Должно быть обязательно заполнено, чтобы избежать ошибок Out of memory и перегрузки отдельных воркеров при старте системы. Рекомендуемое значение &mdash; около 60% от целевого количества воркеров (чтобы пережить потерю части воркеров, например при отключении одного дата-центра).
      "lost_job_timeout":
        description: >-
          Через какое время признавать `Job` потерянным, если в контроллер не приходит обновление статуса джоба. То же время используется, чтобы признавать потерянным воркера.
      "faulty_address_window":
        description: >-
          Окно на котором считается экспоненциально затухающий счетчик числа потерь соединения контроллера с воркером. Если этот счетчик превышает `faulty_address_attempts`, то воркер считается `faulty` и он игнорируется при балансировке.
          Решает проблему воркеров, у которых постоянно теряется коннект с контроллером. В том числе проблему двух воркеров с разными incarnation id и одним address (когда в деплое оказывается два пода считающих, что у них один и тот же FQDN).
      "faulty_address_attempts":
        description: >-
          Смотрите описание параметра `faulty_address_window`.


  "NYT::NFlow::TDynamicJobManagerSpec":
    members:
      "use_cpu_aware_balancer":
        description: >-
          Включает экспериментальную балансировку CPU. Балансировка по умолчанию может приводить к сильному перекосу ресурсов CPU между воркерами.
      "rebalance_sync_period":
        description: >-
          Период `cpu_aware` балансировки. Не рекомендуется выставлять значение меньше `15min`, так как балансировка смотрит на метрики `10min`.
      "rebalance_target_deviation":
        description: >-
          Максимальное допустимое отклонение.


  "NYT::NFlow::TDynamicJobTrackerSpec":
    members:
      "job_control_threads":
        description: |-
          Количество управляющих тредов.

          В большинстве случаев достаточно значения по умолчанию.
      "job_threads":
        description: |-
          Количество тредов для выполнения джобов.

          Если не задано, размер пула вычисляется автоматически на основе CPU-лимита ноды.
          Если при этом вычислить лимит ноды не получилось, то будет создано 30 тредов.
      "buffer_state_manager":
        description: >-
          Конфиг модуля управления буферами для входящих и исходящих сообщений.


  "NYT::NFlow::TDynamicBufferStateManagerSpec::TOneSideBufferSpec":
    members:
      "fair_share_pool":
        description: >-
          Размер пула для распределения по FairShare алгоритму на основании утилизации.
      "job_guarantee":
        description: >-
          Минимальный размер буффера для одной джобы.
      "job_limit":
        description: >-
          Максимальный размер буффера для одной джобы.
      "max_duration":
        description: >-
          Считать, что не нужно держать буффер больше, чем необходимо для max_duration времени работы джобы (скорость оценивается эвристически).
      "job_overrides":
        description: >-
          Возможность переопределить вручную размер буффера для компьютейшен-стрима.


  "NYT::NFlow::TDynamicBufferStateManagerSpec":
    members:
      "input_buffer":
        description: >-
          Настройки входного буффера.
      "output_buffer":
        description: >-
          Настройки выходного буффера.
      "manage_period":
        description: >-
          Частота пересчёта размера буфферов.
      "demand_window":
        description: >-
          Временное окно, на котором оценивается утилизация одного буффера.


  "NYT::NFlow::TDynamicControllerConnectorSpec":
    members:
      "controller_wait_timeout":
        description: >-
          Время, через которое воркер принудительно отключит все джобы в случае потери связи с контроллером.
      "controller_discover_period":
        description: >-
          Период повторного поиска контроллера.
      "controller_heartbeat_period":
        description: >-
          Период хартбитов воркера в контроллер.
      "controller_heartbeat_rpc_timeout":
        description: >-
          Таймаут хартбита.
      "controller_heartbeat_failure_backoff":
        description: >-
          Время выдержки на случай ошибки хартбита.
      "controller_handshake_rpc_timeout":
        description: >-
          Таймаут рукопожатия.
      "controller_heartbeat_failure_backoff":
        description: >-
          Время выдержки на случай ошибки рукопожатия.
      "orchid_update_period":
        description: >-
          Период обновления `orchid` на контроллере.

  "NYT::NFlow::TFlowNodeConfig":
    members:
      "rpc_port":
        description: >-
          Основной порт для общения воркеров и контроллеров, может быть отдельным у каждого инстанса.
      "monitoring_port":
        description: >-
          API для получения метрик.
      "cluster_url":
        description: >-
          Кластер {{product-name}} для работы.
      "proxy_role":
        description: >-
          Роль `rpc proxy`.
      "path":
        description: >-
          Рабочая папка пайплайна на кластере `cluster_url`.
      "controller":
        description: >-
          Параметры `Controller`.
      "worker":
        description: >-
          Параметры `Worker`.
      "tvm":
        internal: true
      "abort_on_unrecognized_options":
        description: >-
          При выставлении значения true Flow Node не будет запускаться при наличии неизвестных опций в конфиге.
      "logging":
        description: >-
          Настройки логирования.
      "enable_phdr_cache":
        advanced: true


  "NYT::NFlow::NController::TControllerConfig":
    members:
      "controller_threads":
        description: >-
          Число тредов.
      "warm_up_time":
        description: >-
          Период разогрева при старте контроллера.
      "scheduler_period":
        description: >-
          Период запуска планировщика.
      "persisted_state_manager":
        description: >-
          Настройки `PersistedStateManager`.
      "lease_manager":
        description: >-
          Настройки `LeaseManager`.
      "controller_service":
        description: >-
          Настройки `ControllerService`.
      "orchid_update_period":
        description: >-
          Период пересчёта `orchid`.
      "publish_retry_period":
        description: >-
          Период, в течение которого контроллер пытается передать в {{product-name}} информацию, что является лидером.
      "election_manager":
        description: >-
          Настройки выбора лидера среди нескольких контроллеров.


  "NYT::NFlow::NController::TPersistedStateManagerConfig":
    members:
      "timeout":
        description: >-
          Таймаут сохранения в {{product-name}}.


  "NYT::NFlow::NController::TLeaseManagerConfig":
    members:
      "lease_timeout":
        description: >-
          Таймаут lease-транзакции.
      "lease_ping_period":
        description: >-
          Период, через который контроллер будет пинговать lease-транзакцию.
      "lease_check_period":
        description: >-
          Период, через который контроллер будет проверять живость lease-транзакции.
      "lease_check_period_jitter":
        description: >-
          Jitter для распределения нагрузки во времени.
      "max_concurrent_requests":
        description: >-
          Максимальное количество одновременных запросов. Не увеличивайте, чтобы избежать перегрузки мастера.


  "NYT::NFlow::NController::TControllerServiceConfig":
    members:
      "set_spec_retry_count":
        description: >-
          Количество попыток обновления спеки в случае ошибок.
      "set_spec_retry_period":
        description: >-
          Время между попытками.


  "NYT::NFlow::TMessageBatcherSettings":
    members:
      "batch_duration":
        description: >-
          Время максимального набора входного батча.
      "max_rows_per_batch":
        description: >-
          Максимальный размер входного батча в строках. Считается отдельно для `input_streams`, `timer_streams` и для каждого из `source_streams`.
      "max_bytes_per_batch":
        description: >-
          Максимальный размер входного батча в байтах. Считается аналогично `max_rows_per_batch`.


  "NYT::NFlow::TDynamicJobBalancerSpec":
    members:
      "use_cpu_aware_balancer":
        description: >-
          Включает экспериментальную балансировку CPU. Балансировка по умолчанию может приводить к сильному перекосу ресурсов CPU между воркерами.
      "rebalance_target_deviation":
        description: >-
          Максимальное допустимое отклонение.
      "rebalance_sync_period":
        description: >-
          Период `cpu_aware` балансировки. Не рекомендуется выставлять значение меньше `15min`, так как балансировка смотрит на метрики `10min`.


  "NYT::TSingletonsConfig":
    members:
      "logging":
        description: >-
          Настройки логирования.
      "address_resolver":
        advanced: true
      "grpc_dispatcher":
        advanced: true
      "tcmalloc":
        advanced: true
      "fiber_manager":
        advanced: true
      "resource_tracker":
        advanced: true
      "tcp_dispatcher":
        advanced: true
      "rpc_dispatcher":
        advanced: true
      "protobuf_interop":
        advanced: true


  "NYT::NFlow::NStaticTableConnector::TTableSourceParameters":
    members:
      "tables":
        description: >-
          Список таблиц для чтения с указанием кластеров. Этот параметр альтернативен `tables_path`.
          Каждый элемент должен быть таблицей: симлинк не разыменовывается до целевой таблицы,
          а любой нетабличный узел приводит к ошибке.
      "tables_path":
        description: >-
          Путь до директории со статическими таблицами с указанием кластера. Этот параметр альтернативен `tables`.
      "table_name_filter":
        description: >-
          Регулярное выражение для фильтрации таблиц по имени: читаются только те таблицы, имя которых совпадает с выражением.
          Синтаксис выражений &mdash; [RE2](https://github.com/google/re2/wiki/Syntax).
      "event_timestamp_locator":
        description: >-
          По умолчанию берёт таймстемп из имени таблицы.
          Это время соответствует времени создания данных, оно будет проброшено в EventTimestamp сообщений.
          Все обрабатываемые таблицы должны иметь разный таймстемп.
          И новые таблицы должны появляться с таймстемпом большим, чем у всех предыдущих таблиц.
      "system_timestamp_locator":
        description: >-
          По умолчанию берёт таймстемп из времени создания таблицы.
          Это время соответствует времени записи данных в сорс.
          То есть моменту, когда пайплайн может увидеть эти данные и начать читать.
          Это время пробрасывается в SystemTimestamp сообщений.
      "ignore_symlinks":
        description: >-
          Флаг, который позволяет игнорировать симлинки внутри папки с таблицами.
      "watermark_delay":
        description: >-
          С какой задержкой двигать ватермарк по source stream.


  "NYT::NFlow::NStaticTableConnector::TDynamicTableSourceParameters":
    members:
      "desired_table_process_time":
        description: >-
          За какое время читать одну входную таблицу. Основной параметр для контроля скорости чтения.
      "restart_instant":
        description: >-
          Установите в этот параметр текущее время в iso8601, чтобы забыть текущий прогресс и начать читать статические таблицы заново.
          Значение параметра можно безопасно уменьшать, это не приведёт к дополнительному рестарту чтения.
          Рестарт происходит, только если текущий restart_instant больше, чем последний restart_instant, что сорс сохранил внутри себя.
          При этом параметр никак не соотносится с таймстемпами статических таблиц и не производит их фильтрацию для рестарта чтения.
      "desired_partition_process_time":
        advanced: true
        description: >-
          Менять не рекомендуется. Регулирует, насколько крупные партиции будет нарезать контроллер.
      "desired_partition_rows_per_second":
        advanced: true
        description: >-
          Менять не рекомендуется. Регулирует, насколько крупные партиции будет нарезать контроллер.
      "desired_partition_bytes_per_second":
        advanced: true
        description: >-
          Менять не рекомендуется. Регулирует, насколько крупные партиции будет нарезать контроллер.
      "min_event_timestamp":
        advanced: true
        description: >-
          Таблицы у которых EventTimestamp меньше MinEventTimestamp, не будут процесситься.
      "max_partition_count":
        advanced: true
        description: >-
          Менять не рекомендуется. Ограничение сверху на число одновременно живущих партиций.
      "throttler_period":
        advanced: true
        description: >-
          Менять не рекомендуется. Регулирует окно на котором регулируется скорость чтения из партиции.
      "read_timeout":
        advanced: true
        description:
          Менять не рекомендуется. Пересоздает table reader, если тот возвращает пустой ответ в течение периода.


  "NYT::NFlow::TOrderedSourceBase::TExtendedDynamicParameters":
    members:
      "unavailable_time_half_decay_period":
        advanced: true

  "NYT::NFlow::TDynamicUnitedParameters<NYT::NFlow::NStaticTableConnector::TSource>":
    advanced_parameters_annotation: >-
      Эти параметры для тонкой настройки, не рекомендуется трогать без глубокого понимания системы.

  "NYT::NFlow::TOrderedSourceBase::TExtendedParameters":
    members:
      "finite":
        description: >-
          Считать source конечным, то есть запомнить число сообщений в нём при старте и перевести stream в состояние completed при вычитывании этого числа сообщений.
      "update_info_period":
        advanced: true
      "byte_size_alpha":
        advanced: true


  "NYT::NFlow::TSwiftMapComputation::TExtendedParameters":
    members:
      "allow_batching_with_relaxed_guarantees":
        description: >-
          Разрешает склейку нескольких входных сообщений в одно выходное (батчинг). Полезно для свёртки множества мелких сообщений в одно более крупное, чтобы снизить нагрузку по числу сообщений на партицию ниже по конвейеру.
          По умолчанию `%false`: каждое выходное сообщение порождается ровно одним родителем, `MessageId` детерминированно наследуется от родителя, что даёт exactly-once при детерминированных пользовательских функциях.
          При `%true` выходное сообщение может иметь несколько родителей; один родитель считается обработанным, только когда обработаны все его дети. На повторных запусках границы склейки могут отличаться, поэтому один и тот же родитель может попасть в несколько разных склеенных детей — нижестоящие вычисления должны быть готовы видеть содержимое каждого родителя более одного раза (семантика at-least-once).
          Кроме того, склеенные выходные сообщения получают новый `MessageId` (через `UniqueSeqNo`), не унаследованный от какого-либо родителя. Это значит, что порядок сообщений по `MessageId` в рамках одного ключа на стороне нижестоящих `Computation` может нарушаться: если последующие звенья пайплайна полагаются на упорядоченность по `MessageId` внутри ключа, такую логику нужно переписать с учётом этого.


  "NYT::NFlow::TSimpleRunnerConfig":
    members:
      "cluster_url":
        description: >-
          Имя кластера, на котором находится пайплайн.
      "proxy_role":
        description: >-
          [Прокси-роль](../../user-guide/proxy/about#rpc_proxy), которую нужно использовать для выполнения операций над пайплайном.
      "path":
        description: >-
          Путь к пайплайну на {{product-name}}.
      "spec":
        description: >-
          Статическая спека, с которой нужно запустить пайплайн.
      "dynamic_spec":
        description: >-
          Динамическая спека, с которой нужно запустить пайплайн.
      "vanilla":
        description: >-
          Если задан и `enable=%true`, раннер вместо запуска контроллера/воркеров локально
          поднимет vanilla-операцию в {{product-name}} и запустит федерацию в ней.

  "NYT::NFlow::TVanillaTaskConfig":
    members:
      "count":
        description: >-
          Число джоб задачи (контроллера или воркера) в vanilla-операции.
      "memory_limit":
        description: >-
          Лимит памяти на одну джобу. Если не задан, подставляется дефолт задачи (свой у контроллера и воркера).
      "cpu_limit":
        description: >-
          Лимит CPU на одну джобу. Если не задан, подставляется дефолт задачи (свой у контроллера и воркера).
      "port_count":
        description: >-
          Сколько портов запросить у YT (порты выдаются через `YT_PORT_<i>` и перекрывают фиксированные порты из node-config). Нужно на хосте с общей сетью, где фиксированные порты соседних джоб столкнулись бы.

  "NYT::NFlow::TVanillaConfig":
    members:
      "enable":
        description: >-
          Включить vanilla-режим. Если `%false`, блок игнорируется.
      "pool":
        description: >-
          Пул, в котором запускается vanilla-операция.
      "controller":
        description: >-
          Конфиг джоб контроллера (число джоб и лимиты ресурсов). Если не задан, используется одна джоба с дефолтными лимитами.
      "worker":
        description: >-
          Конфиг джоб воркеров (число джоб и лимиты ресурсов). Обязательно нужно указать `count`.
      "runtime_cluster":
        description: >-
          Кластер, на котором запускается vanilla-операция. По умолчанию совпадает с `cluster_url` пайплайна.
      "runtime_proxy_role":
        description: >-
          Роль RPC-прокси для `runtime_cluster` (роль кластера пайплайна может не существовать на нём).
      "cache_path":
        description: >-
          Content-addressed кэш, в который заливаются файлы джоб (общий для всех flow-операций на кластере).
          Долговременная копия в папке пайплайна — дешёвый `CopyNode` отсюда.
      "wait_timeout":
        description: >-
          Таймаут ожидания состояний пайплайна при штатном останове прежней vanilla-операции.
      "max_failed_job_count":
        description: >-
          Максимальное число упавших джобов, при достижении которого vanilla-операция падает.
      "solomon_resolver_tag":
        description: >-
          Тег для solomon-резолвера, прописываемый в аннотации операции.
      "alias":
        description: >-
          Явный alias vanilla-операции. Если не задан, генерируется как `*flow-runner <cluster>:<path>`.
      "title":
        description: >-
          Title vanilla-операции.
      "network_project":
        description: >-
          Сетевой проект, в котором запускается vanilla-операция.
      "proxy_url_aliasing_rules":
        description: >-
          Алиасы прокси-URL, прокидываемые во flow-server внутри vanilla-джоб.
      "node_config":
        description: >-
          Произвольный YSON-патч, накладываемый поверх дефолтного `TFlowNodeConfig` внутри vanilla-джоб.

  "NYT::NFlow::TMessageSerializer":
    members:
      "message_id":
        description: >-
          Уникальный id сообщения.
      "system_timestamp":
        description: >-
          Timestamp создания конкретного сообщения.
      "event_timestamp":
        description: >-
          Timestamp реального события, ассоциированного с данным сообщением.
      "stream_id":
        description: >-
          Поток, в рамках которого существует данное сообщение.
      "payload":
        description: >-
          Непосредственно данные.
      "payload_schema":
        description: >-
          Схема payload, заполняется на базе `stream_id`.

  "NYT::NFlow::TTimerSerializer":
    members:
      "message_id":
        description: >-
          Уникальный id таймера.
      "system_timestamp":
        description: >-
          Timestamp создания таймера.
      "event_timestamp":
        description: >-
          Timestamp реального события, ассоциированного с данным таймером.
      "stream_id":
        description: >-
          Поток, в рамках которого существует данное сообщение.
      "key":
        description: >-
          Ключ таймера.
      "key_schema":
        description: >-
          Схема ключа, совпадает с `group_by_schema` соответствующего `Computation`.
      "trigger_timestamp":
        description: >-
          Время для срабатывания таймера.


  "NYT::NFlow::TDynamicStateCacheSpec":
    members:
      "uncompressed_cache_weight":
        description: >-
          Ограничение на `uncompressed`.
      "compressed_cache_weight":
        description: >-
          Ограничение на `compressed`.


  "NYT::NFlow::NBigRTExtensions::TProfileJoinerSpec":
    members:
      "path":
        description: >-
          Путь к динтаблице со стейтами.


  "NYT::NFlow::NBigRTExtensions::TDynamicProfileJoinerSpec":
    members:
      "reset_malformed_state":
        description: >-
          Иногда в коде возникают проезды по памяти и другие баги, которые могут приводить к повреждению произвольных данных в программе.
          В результате могут побиться стейты и некорректно записаться в {{product-name}}. При подгрузке битых стейтов будет бросаться исключение и обработка встанет.
          Данная опция позволяет считать битые стейты пустыми и продолжать обработку. По сути она аварийная опция, которая не должна работать в нормальном режиме.
      "discard_unknown_fields":
        description: >-
          Опция включает сброс неизвестных полей протобуфа.


  "NYT::NFlow::NBigRTExtensions::TProfileManagerSpec":
    members:
      "path":
        description: >-
          Путь к динтаблице со стейтами.


  "NYT::NFlow::TExternalStateManagerSpec":
    members:
      "external_state_manager_class_name":
        description: |-
          Полное имя класса external state manager'а. Должен быть зарегистрирован макросом `YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER` (или быть библиотечной реализацией, как `NYT::NFlow::TSimpleExternalStateManager`).
      "parameters":
        description: >-
          Параметры конкретной реализации menager'а. Для `TSimpleExternalStateManager` — `TSimpleExternalStateManagerSpec`, для `TProfileManager<TProfile>` — `TProfileManagerSpec` и т.д.


  "NYT::NFlow::TDynamicExternalStateManagerSpec":
    members:
      "parameters":
        description: >-
          Динамические параметры конкретной реализации менеджера.


  "NYT::NFlow::TExternalStateJoinerSpec":
    members:
      "external_state_joiner_class_name":
        description: |-
          Полное имя класса external state joiner'а. Должен быть зарегистрирован макросом `YT_FLOW_DEFINE_EXTERNAL_STATE_JOINER` (или быть библиотечной реализацией, как `NYT::NFlow::TSimpleExternalStateJoiner`).
      "parameters":
        description: >-
          Параметры конкретной реализации joiner'а.
      "client_provider_resource_id":
        description: |-
          Если задан, фреймворк ищет статический ресурс с этим именем (должен реализовывать `IYTClientProvider`) и использует его `Get()` как YT-клиент joiner'а. При этом `cluster` из rich path таблицы игнорируется. Взаимоисключающе с `client_factory_resource_id` (этот параметр приоритетнее).
      "client_factory_resource_id":
        description: |-
          Если задан, фреймворк ищет статический ресурс с этим именем (должен реализовывать `IYTClientProvider`) и использует `GetClient(cluster)` для получения YT-клиента под кластер из rich path таблицы. Взаимоисключающе с `client_provider_resource_id`.
      "join_on":
        description: |-
          Описание соответствия между ключами входного потока computation'а и ключами joiner'а (см. `TStateJoinSpec`).
      "auto_preload":
        description: |-
          Если `true` (по умолчанию), фреймворк сам вызывает `PreloadKeyStates` у этого joiner'а перед каждым `DoProcess`, формируя ключи по `join_on`. Если `false`, computation сам отвечает за вызов `Client.PreloadKeyStates(IInputContextPtr)` или `PreloadKeyStates(THashSet<TKey>)` до `GetState`.


  "NYT::NFlow::TStateJoinSpec":
    members:
      "key_schema_override":
        description: |-
          Если задана, схема используется в качестве ключа joiner'а вместо `group_by_schema` computation'а.
      "key_provider_streams":
        description: |-
          Если задано, ключи для joiner'а берутся только из сообщений и таймеров с указанными `stream_id`; `nullopt` означает «из всех входных стримов computation'а».


  "NYT::NFlow::TDynamicExternalStateJoinerSpec":
    members:
      "parameters":
        description: >-
          Динамические параметры конкретной реализации joiner'а.


  "NYT::NFlow::TStateJoinerSpec":
    members:
      "computation_id":
        description: |-
          `Computation`, чей внутренний стейт читается этим joiner'ом.
      "state_name":
        description: |-
          Имя стейт-клиента целевого `Computation`'а — тот префикс, который он передал в `InitClient` (начинается с `/`).
      "join_on":
        description: |-
          Описание соответствия между ключами входного потока computation'а и ключом стейта целевого `Computation`'а (см. `TStateJoinSpec`). `key_schema_override` должна совпадать с `group_by_schema` целевого `Computation`'а.
      "auto_preload":
        description: |-
          Если `true` (по умолчанию), фреймворк сам вызывает `PreloadKeyStates` у этого joiner'а перед каждым `DoProcess`, формируя ключи по `join_on`. Если `false`, computation сам отвечает за вызов `PreloadKeyStates` до `GetState`.


  "NYT::NFlow::TDynamicStateJoinerSpec":
    members:
      "cache":
        description: >-
          Настройки TTL-кэша загруженных стейтов. По умолчанию выключен (`ttl = 0`).


  "NYT::NFlow::TSimpleExternalStateManagerSpec":
    members:
      "path":
        description: >-
          Путь до динтаблицы со стейтами (с указанием кластера в rich path).


  "NYT::NFlow::TSimpleExternalStateJoinerSpec":
    members:
      "path":
        description: >-
          Путь до динтаблицы со стейтами (с указанием кластера в rich path).


  "NYT::NFlow::TDynamicSimpleExternalStateJoinerSpec":
    members:
      "cache":
        description: >-
          Настройки TTL-кэша загруженных стейтов.


  "NYT::NFlow::TStaticTableKeyVisitorJoinerSpec":
    members:
      "path":
        description: >-
          Путь до статической сортированной таблицы-источника (с указанием кластера в rich path).
          Префикс её ключевых колонок должен совпадать с `group_by_schema` computation'а по именам и типам;
          вычисляемая (партиционная) колонка должна быть материализована значениями своего выражения.
      "unavailable_source_policy":
        description: |-
          Поведение при неуспешном чтении источника (исчерпавшем `read_attempts` попыток).
          `retry` (по умолчанию) — ошибка роняет итерацию обхода и чтение повторяется; обход не продвигается за непрочитанный диапазон.
          `mark_unreadable` — ошибка проглатывается, обход идёт дальше, а ключи непрочитанного диапазона резолвятся неинициализированным аксессором (`IsInitialized() == false`).


  "NYT::NFlow::TDynamicStaticTableKeyVisitorJoinerSpec":
    members:
      "read_attempts":
        description: >-
          Бюджет попыток одного чтения источника; чтение, исчерпавшее бюджет, считается неуспешным
          и обрабатывается согласно `unavailable_source_policy`.
      "unavailable_source_backoff":
        description: |-
          На какое время неуспешное чтение помечает источник недоступным.
          Пока пометка действует, обращений к источнику нет: каждое чтение немедленно резолвится согласно `unavailable_source_policy`;
          первое чтение после истечения окна снова пробует источник.


  "NYT::NFlow::NBigRTExtensions::TDynamicProfileManagerSpec":
    members:
      "codec":
        description: >-
          Опция позволяет выбрать кодек сжатия для базовых колонок и алгоритм дельта-кодирования. Дефолт опции достаточно хороший (zstd6 + vcdiff). Не рекомендуется менять значение опции без консультации с разработчиками BigRT/Flow.
      "recode_probability":
        description: >-
          Опция для ограничения скорости рекодирования профилей, используется только при замене кодека или алгоритма дельта-кодирования.
      "reset_malformed_state":
        description: >-
          Иногда в коде возникают проезды по памяти и другие баги, которые могут приводить к повреждению произвольных данных в программе.
          В результате могут побиться стейты и некорректно записаться в {{product-name}}. При подгрузке битых стейтов будет бросаться исключение и обработка встанет.
          Данная опция позволяет считать битые стейты пустыми и продолжать обработку. По сути это аварийная опция, которая не должна работать в нормальном режиме.
      "discard_unknown_fields":
        description: >-
          Опция включает сброс неизвестных полей протобуфа.
      "enable_corruption_diagnostics":
        description: >-
          Единый переключатель, включающий логирование, помогающее диагностировать повреждения стейтов.
          Если опция выставлена, её значение переопределяет `row_load_options.enable_versioned_lookup_logging` и `row_write_options.enable_modifications_logging`.
          Если значение `true`, дополнительно принудительно включается `row_load_options.use_versioned_lookup` (versioned-lookup-логирование работает только при versioned-lookup).
          Если опция не выставлена, действуют значения соответствующих опций, заданные отдельно.
          Все диагностические сообщения пишутся в отдельный логгер `BigRTSerializableProfileCorruptionDiagnostic`.


  "NYT::NFlow::TTableFetcherSpec":
    members:
      "table_path":
        description: >-
          Путь к таблице с указанием кластера (можно указать несколько кластеров).
      "value_columns":
        description: >-
          Неключевые колонки, которые нужно читать (по умолчанию читаются все колонки).
      "fetch_type":
        description: >-
          Важный параметр, влияет на характер нагрузки на YT, см. описание EFetchType.


  "NYT::NFlow::TFetcherInJoinerSpec":
      members:
        "prefix":
          description: >-
            Префикс имени колонки, который будет добавлен для неключевых колонок таблицы.


  "NYT::NFlow::TTableJoinerSpec":
      members:
        "fetchers":
          description: >-
            Спеки фетчеров.


  "NYT::NFlow::TServiceLogParameters":
    members:
      "table_joiner":
        description: >-
          Спека джойнера.


  "NYT::NFlow::TDynamicServiceLogParameters":
    members:
      "desired_partition_count":
        description: >-
          Желаемое количество партиций. Можно оценить как 1 партиция на 1 MB/s потока.
      "desired_cycle_time":
        description: >-
          Cорс будет стремиться поддерживать такую скорость генерации сообщений, чтобы обходить всю таблицу раз в `desired_cycle_time`.
      "throttler_period":
        advanced: true
        description: >-
          Период троттлинга.


  "NYT::NFlow::TQueueInfoSpec":
    members:
      "queue_path":
        description: >-
          Путь до очереди с указанием кластера.
      "update_partition_count_period":
        advanced: true
        description: >-
          Как часто контроллеру обновлять число партиций в очереди (контроллер меняет число партиций computation в соответствии с числом партиций в очереди).


  "NYT::NFlow::TQueueSourceParameters":
    members:
      "consumer_path":
        description: >-
          Путь до консьюмера очереди с указанием кластера.
      "try_parse_flow_queue_meta":
        description: >-
          Нужно ли брать и парсить мету из очереди (в мете лежат ватермарки предоставленные писателем в очередь).
      "flow_queue_meta_column":
        description: >-
          Из какой колонки брать мету.
      "ignore_malformed_flow_queue_meta":
        description: >-
          Игнорировать невалидную мету или падать.


  "NYT::NFlow::TDynamicQueueSourceParameters":
    members:
      "pull_queue_timeout":
        description: >-
          Таймаут для запроса чтения из очереди.


  "NYT::NFlow::TCommonQueueSinkParameters":
    members:
      "write_flow_queue_meta":
        description: >-
          Нужно ли писать мету в очередь (в мете пишутся ватермарки).
      "flow_queue_meta_column":
        description: >-
          В какую колонку писать мету.


  "NYT::NFlow::TDynamicCommonQueueSinkParameters":
    members:
      "flow_queue_meta_heartbeat_period":
        advanced: true
        description: >-
          Как часто контроллер будет во все партиции очереди писать хартбиты с метой с ватермарками.


  "NYT::NFlow::TSyncQueueSinkParameters":
    members:
      "column_filter":
        description: >-
          Какие колонки из сообщения писать в очередь (по умолчанию &mdash; все).


  "NYT::NFlow::TAsyncQueueWriterParametersBase":
    members:
      "producer_path":
        description: >-
          Путь до продюсера очереди с указанием кластера.
      "require_sync_replica":
        description: >-
          Одноименный параметр при записи в очередь. Разрешена ли запись в очередь без синхронных реплик.

  "NYT::NFlow::TAsyncMultiClusterQueueWriterParameters":
    members:
      "use_clusters":
        description: >-
          Использовать ли `<clusters=[...]>` вместо `<cluster=...>` из rich path в producer и queue


  "NYT::NFlow::TDynamicAsyncQueueWriterParameters":
    members:
      "write_period":
        advanced: true
      "max_rows_per_write":
        advanced: true
      "max_bytes_per_write":
        advanced: true
      "backoff_duration":
        advanced: true


  "NYT::NFlow::TAsyncQueueSinkParametersBase":
    members:
      "column_filter":
        description: >-
          Какие колонки из сообщения писать в очередь (по умолчанию &mdash; все).


  "NYT::NFlow::NSortedDynamicTable::TInfoSpec":
    members:
      "table_path":
        description: >-
          Путь к целевой сортированной динамической таблице с указанием кластера.
      "update_partition_count_period":
        advanced: true
        description: >-
          Как часто контроллеру обновлять число таблетов в таблице (контроллер меняет число каналов получателя в соответствии с числом таблетов).

  "NYT::NFlow::NSortedDynamicTable::TSyncSinkParameters":
    members:
      "column_filter":
        description: >-
          Какие колонки из сообщения писать в таблицу (по умолчанию &mdash; все). При использовании параметра `delete_rows` необходимо указать ключевые колонки целевой таблицы, иначе удаления будут падать.
      "aggregate_columns":
        description: |-
          Список колонок, для которых при записи учитывается функция агрегации. По умолчанию происходит перезапись значения в колонке.
      "delete_rows":
        description: |-
          Если установлено в `true`, то вместо вставки строк будет выполняться их удаление. В этом режиме важно, чтобы в `column_filter` были выбраны ключевые колонки целевой таблицы.

          {% note info %}

          Sink может или работать в режиме вставки, или удаления, но не одновременно. Если вам нужен функционал и вставки, и удаления, то используйте StateManager.

          {% endnote %}
      "require_sync_replica":
        description: |-
          Если установлено в `false`, то при записи строк в целевую таблицу не проверяется наличие синхронной реплики. По умолчанию значение `true` - проверка происходит.


  "NYT::NFlow::TStateAccessArgs":
    members:
      "computation_id":
        description: >-
          Идентификатор Computation'а. Обязателен для режимов 1 и 2 (см. описание самой команды).
      "partition_id":
        description: >-
          Идентификатор партиции. Обязателен для режима 3 (см. описание самой команды).
      "key":
        description: |-
          Ключ для точечного lookup'а (режим 2). Принимается в двух формах:

          * **YSON-словарь** `{column = value; ...}` &mdash; для каждой целевой таблицы (`key_states` Computation'а и каждой таблицы external_state_manager'а/joiner'а с собственной `key_schema_override`) словарь раскладывается по её ключевой схеме; колонки, отсутствующие в схеме, игнорируются.
          * **YSON-список** позиционных значений &mdash; передаётся в таблицы как есть, без адаптации под их схему.
      "name":
        description: >-
          Фильтр по точному имени стейта; применяется ко всем секциям (key/partition/external). Если задан, в каждой секции возвращаются только строки с этим именем стейта.
      "target":
        description: >-
          Какие категории стейтов запрашивать. По умолчанию `all`.


  "NYT::NFlow::TReadStatesArg":
    description: |-
      Аргумент команды `read-states`.

      Команда поддерживает три режима использования (определяются набором заполненных полей):

      1. Только `computation_id` &mdash; все стейты для всех партиций указанного Computation'а.
      2. `computation_id` + `key` &mdash; точечный lookup по ключу в `key_states` (и в external-стейтах) указанного Computation'а.
      3. Только `partition_id` &mdash; все `partition_states` указанной партиции. Дополнительно, если эта партиция принадлежит `SourceComputation` (каждая такая партиция отвечает ровно за один `SourceKey`, в отличие от обычных партиций с диапазоном `[LowerKey; UpperKey)`), сразу же подгружаются и `key_states` под этим `SourceKey`.
    members:
      "limit":
        description: |-
          Максимальное число строк, возвращаемых в каждой секции ответа независимо.
          Под секциями имеются в виду поля ответа: `key_states`, `partition_states`, `external_key_states`, `joined_external_key_states`. У каждой свой бюджет в `limit` строк.

          Внутри секций `external_key_states` / `joined_external_key_states` бюджет честно делится между активными external-стейтами этого Computation'а.


  "NYT::NFlow::TDeleteStatesArg":
    description: |-
      Аргумент команды `delete-states`.

      Поддерживаются те же три режима, что и в [TReadStatesArg](./all_yson_structs.md#NYT_NFlow_TReadStatesArg).

      По умолчанию dry-run: запрос возвращает счётчики совпавших строк, ничего не удаляя. Для реального удаления необходимо передать `commit=true`.

      Команда требует, чтобы пайплайн был в состоянии `Stopped`/`Completed`, либо `force=true` при `Paused`. Удаляются только key/partition/manager-стейты; joiner-стейты не трогаются.
    members:
      "commit":
        description: >-
          Если `true`, найденные строки действительно удаляются. Иначе запрос работает в dry-run-режиме и только подсчитывает их.
      "force":
        description: >-
          Разрешает удаление на пайплайне в состоянии `Paused`. Без `force` команда требует `Stopped`/`Completed`.


  "NYT::NFlow::TKeyStateRow":
    description: |-
      Одна строка в секциях `key_states`, `external_key_states`, `joined_external_key_states` ответа `read-states`.
    members:
      "computation_id":
        description: >-
          Идентификатор Computation'а, которому принадлежит ключ.
      "key":
        description: >-
          Значение ключа (YSON-словарь `{column = value; ...}` по `key_schema`).
      "states":
        description: |-
          Содержимое стейтов под этим ключом: словарь `имя_стейта → YSON-значение`. YSON-значение здесь — это сериализованный пользовательский payload стейта (тип определяется конкретным `State`/`ExternalState` в коде Computation'а; для встроенных типов вроде счётчиков это просто число, для произвольных пользовательских структур &mdash; YSON-словарь со всеми их полями).

          Для секций `external_key_states` / `joined_external_key_states` имя стейта совпадает с именем external_state-клиента (то, что передаётся в `IJobInitContext::InitExternalStateClient`, например `/state`).


  "NYT::NFlow::TPartitionStateRow":
    description: |-
      Одна строка в секции `partition_states` ответа `read-states`.
    members:
      "computation_id":
        description: >-
          Идентификатор Computation'а. Может отсутствовать, если партиция не привязана к конкретному Computation'у.
      "partition_id":
        description: >-
          Идентификатор партиции.
      "states":
        description: >-
          Содержимое partition-стейтов: мапа `имя_стейта → YSON-значение`.


  "NYT::NFlow::TReadStatesResponse":
    description: |-
      Ответ команды `read-states`. Каждая секция наполняется независимо по своему `limit`.
    members:
      "key_states":
        description: >-
          Стейты, привязанные к ключам (по ключевой схеме Computation'а).
      "partition_states":
        description: >-
          Стейты, привязанные к партициям.
      "external_key_states":
        description: >-
          Стейты из external_state_manager'ов (mutable, могут быть удалены через `delete-states`).
      "joined_external_key_states":
        description: >-
          Стейты, наблюдаемые через external_state_joiner'ы (read-only, join-источники).
      "errors":
        description: >-
          Ошибки, возникшие при формировании ответа на сам запрос (например, не удалось адаптировать ключ под схему конкретного external-стейта или упал `List`/`Lookup` в его таблицу). Не относятся к ошибкам в работе пайплайна со стейтом. Если такая ошибка случилась для одного из external-стейтов, остальные секции всё равно наполняются успешно.


  "NYT::NFlow::TMatchedStatesBucket":
    description: |-
      Подсчёт совпавших строк по одной из категорий ответа `delete-states`.
    members:
      "total":
        description: >-
          Суммарное число совпавших строк в этой категории.
      "details":
        description: >-
          Разбивка `computation_id → имя_стейта → число строк`.


  "NYT::NFlow::TMatchedStates":
    description: |-
      Разбивка совпавших строк по категориям. Используется в [TDeleteStatesResponse](./all_yson_structs.md#NYT_NFlow_TDeleteStatesResponse).
    members:
      "key_states":
        description: >-
          Совпавшие строки в `key_states`.
      "partition_states":
        description: >-
          Совпавшие строки в `partition_states`.
      "external_key_states":
        description: >-
          Совпавшие строки в external_state_manager'ах.


  "NYT::NFlow::TDeleteStatesResponse":
    description: |-
      Ответ команды `delete-states`.
    members:
      "committed":
        description: |-
          Отражает значение `commit` из аргумента запроса. Если в `errors` есть записи, часть совпавших строк могла не удалиться &mdash; точные счётчики не предоставляются, на практике стейт необходимо перечитать через `read-states`.
      "matched_states":
        description: >-
          Счётчики совпавших строк по категориям. Заполняется всегда: и в dry-run-режиме (показывает, что было бы удалено), и при `commit=true` (показывает, что было удалено или попало под попытку удаления). См. [TMatchedStates](./all_yson_structs.md#NYT_NFlow_TMatchedStates).
      "errors":
        description: >-
          Ошибки, возникшие при формировании ответа на сам запрос (например, упал `Sync` манагера). Не относятся к ошибкам в работе пайплайна со стейтом. Если такая ошибка случилась для одного из external-стейтов, остальные категории всё равно могут быть обработаны успешно.


  "NYT::NFlow::TMoniumQuerySpec":
    members:
      "name":
        description: >-
          Имя запроса. Используется для идентификации результата в `response_per_query` и попадает в выходной поток в колонку `query_name`.
      "selector":
        description: >-
          Селектор метрик в формате `{key="value", ...}`. Источник передаёт его в поле `Query.value` запроса `DataService.Read`.


  "NYT::NFlow::TMoniumMetricParameters":
    members:
      "metric_type":
        description: >-
          Тип метрики: `DGAUGE`, `IGAUGE`, `COUNTER` или `RATE`. Определяет, в какое поле `Metric` (`double_value` или `int_value`) пишется значение и какие проверки типа делает sink при разборе колонки.
      "metric_value_column":
        description: >-
          Имя колонки во входной строке, значение которой пишется в `Metric`. Это же имя помещается в метку с ключом `metric_name_label` (см. ниже) и используется как имя сенсора в Monium.
      "labels":
        description: >-
          Список имён колонок, значения которых добавляются как метки сенсора. По умолчанию пусто.


  "NYT::NFlow::TMoniumDriverConfig":
    members:
      "endpoint":
        description: >-
          Адрес gRPC-сервера Monium (host:port).
      "secure":
        description: >-
          Использовать TLS для подключения.
      "auth_mode":
        description: >-
          Режим аутентификации: `Tvm`, `OAuthEnv` или `Iam`. См. также раздел [Настройка аутентификации](../release/basic-rules.md#authentication).
      "tvm_alias":
        description: >-
          Алиас TVM-клиента, который драйвер использует для получения тикета в режиме `Tvm`. Должен соответствовать алиасу, зарегистрированному в TVM-клиенте пайплайна.
      "oauth_env_var":
        description: >-
          Имя переменной окружения с OAuth-токеном для режима `OAuthEnv`. По умолчанию `MONIUM_TOKEN`. Pipeline-authenticator выставляет в воркере как `MONIUM_TOKEN`, так и `SOLOMON_TOKEN` (для обратной совместимости).
      "iam_metadata_url":
        description: >-
          Зарезервировано для будущей реализации discovery через IAM metadata-сервер. На текущий момент режим `Iam` читает токен только из переменной окружения `IAM_TOKEN`; установка непустого значения приведёт к ошибке при загрузке драйвера.
      "network_thread_count":
        description: >-
          Количество потоков gRPC-клиента, обслуживающих unary-вызовы драйвера.
      "stop_before_destroying":
        description: >-
          Если `true`, gRPC-пул будет остановлен до уничтожения ресурса (drain in-flight callbacks). По умолчанию `true`.


  "NYT::NFlow::TMoniumSinkControllerParameters":
    members:
      "channel_count":
        description: >-
          Количество каналов приёмника (`receiver channels`). Если не задано, используется значение по умолчанию фреймворка.


  "NYT::NFlow::TMoniumSinkParameters":
    members:
      "driver_resource_id":
        description: >-
          ID ресурса `TMoniumDriver` в спеке пайплайна. По умолчанию `MoniumDriver`.
      "project":
        description: >-
          Проект Monium, в который пишется метрика.
      "cluster":
        description: >-
          Имя кластера в нотации Monium.
      "service":
        description: >-
          Имя сервиса в нотации Monium.
      "metrics":
        description: >-
          Список конфигов метрик. Каждый элемент описывает маппинг одной строки входного потока в одно сообщение `Metric` (`metric_value_column`, `metric_type`, `labels`).
      "timestamp_column":
        description: >-
          Колонка с типом `Timestamp` или `Uint64` (микросекунды с эпохи), значение которой пишется в `Metric.timestamp`.
      "metric_name_label":
        description: >-
          Метка, в которую помещается имя сенсора (значение `metric_value_column`). По умолчанию `sensor`.
      "skip_null_labels":
        description: >-
          Пропускать строки, в которых хотя бы одна label-колонка равна `null`. По умолчанию `true`.
      "skip_metrics_with_null_timestamp":
        description: >-
          Пропускать строки с пустой временной меткой. По умолчанию `true`.
      "write_timeout":
        description: >-
          Таймаут одного `MetricsDataService.Write` вызова. Применяется как per-call gRPC deadline.


  "NYT::NFlow::TDynamicMoniumSinkParameters":
    members:
      "backoff_parameters":
        description: >-
          Параметры экспоненциального бэкоффа для retry на gRPC-ошибки.
      "write_request_timeout":
        description: >-
          Динамический override per-call gRPC deadline'а на `MetricsDataService.Write`. Если не задан, используется статический `write_timeout` из `TMoniumSinkParameters`.


  "NYT::NFlow::TMoniumSourceParameters":
    members:
      "driver_resource_id":
        description: >-
          ID ресурса `TMoniumDriver` в спеке пайплайна. По умолчанию `MoniumDriver`.
      "project_id":
        description: >-
          Проект Monium, из которого читаются метрики.
      "cluster":
        description: >-
          Кластер (обычно подставляется в селектор; не используется драйвером напрямую).
      "service":
        description: >-
          Сервис.
      "queries":
        description: >-
          Список запросов: каждый элемент — пара `name` (имя для идентификации в ответе) + `selector` (селектор метрик).
      "poll_interval":
        description: >-
          Период поллера и одновременно ширина окна одного поллинга. Каждый раз поллер опрашивает Monium за полуоткрытый интервал `[LastPolledTo, min(now, LastPolledTo + poll_interval))`; после успешного ответа `LastPolledTo` сдвигается ровно на верхнюю границу — окна стыкуются без дубликатов и пропусков. При перезапуске пайплайна `LastPolledTo` восстанавливается из `GetPersistedEventWatermark()`, поэтому downtime произвольной длительности не приводит к потере точек — источник догонит данные за `⌈downtime / poll_interval⌉` поллингов.
      "lookback":
        description: >-
          Глубина истории, которую поллер запросит при холодном старте, когда персистированный watermark ещё недоступен. На холодном старте `LastPolledTo` инициализируется как `now - lookback`. По умолчанию `0` — старт начинается со свежих данных, без догона истории. После первого закоммиченного heartbeat-а параметр больше не используется: позиция восстанавливается из `PersistedEventWatermark`.
      "read_timeout":
        description: >-
          Таймаут одного `DataService.Read` вызова.
      "emit_per_point":
        description: >-
          Если `true` (по умолчанию), для каждой точки временного ряда порождается отдельное сообщение, и колонки `timestamp`/`value` имеют конкретные типы (`Uint64`/`Double`). Если `false`, один временной ряд эмитится одним сообщением с упакованными в `Any` массивами таймстемпов и значений.
      "output_name_column":
        description: >-
          Имя колонки в выходном потоке для имени сенсора. По умолчанию `name`.
      "output_query_name_column":
        description: >-
          Имя колонки для имени запроса (`Query.name` из спеки). По умолчанию `query_name`.
      "output_labels_column":
        description: >-
          Имя колонки для меток. По умолчанию `labels`.
      "output_timestamp_column":
        description: >-
          Имя колонки для временной метки. По умолчанию `timestamp`.
      "output_type_column":
        description: >-
          Имя колонки для типа метрики (строка `DGAUGE`, `IGAUGE`, `COUNTER`, `RATE`). По умолчанию `type`.
      "output_value_column":
        description: >-
          Имя колонки для значения метрики. По умолчанию `value`.


  "NYT::NFlow::TDynamicMoniumSourceParameters":
    members:
      "backoff_parameters":
        description: >-
          Параметры экспоненциального бэкоффа для retry на gRPC-ошибки.
      "read_from_instant":
        description: >-
          Принудительно начать чтение с этого момента (формат ISO-8601). Если задано — переопределяет позицию, восстановленную из персистентного состояния.
      "max_lookback":
        description: >-
          Верхняя граница длительности `lookback` при холодном старте.


enums:
  "NYT::NFlow::EProcessingMode":
    values:
      "exactly_once":
        description: >-
          Значение по умолчанию. Результат работы `Transform`, включая выходные сообщения, обработанные сообщения и т.п. - коммитится в `{{product-name}}` в рамках одной транзакции. При этом все входные сообщения дедуплицируются по `message_id`.
      "at_least_once_consistent":
        description: >-
          В данном режиме отключается дедупликация входных сообщений. В этом режиме `TransformComputation` перестает взаимодействовать с таблицей `input_messages`.
          Входные сообщения могут быть обработаны несколько раз, при этом результат обработки каждый раз будет сохранен в `output_messages` и гарантированно будет обработан следующими `Computation`.
          Этот режим можно использовать, если дубли не являются проблемой, например, так как пользовательская логика сама умеет дедуплицировать лишние сообщения.
          Дубли могут возникать при любых рестартах/падениях джобов (в том числе при решедулинге). Остановка системы через `draining` с помощью `stop-pipeline`, однако, к нарушению гарантий не приводит (если только в процессе не рестартовали джобы по какой-то иной причине).


  "NYT::NFlow::EPipelineState":
    values:
      "unknown":
        description: >-
          Дефолтное значение состояния для еще не запущенного пайплайна.
      "stopped":
        description: >-
          Пайплайн остановлен. Выполнен `draining` &mdash; все промежуточные сообщения в пайплайне обработаны, актуальные обработанные оффсеты в очереди-источники закоммичены. Все джобы остановлены.

          В этом состоянии можно безопасно катить релиз пайплайна и обновлять его статическую спеку.

          Запустить запуск перехода в это состояние можно командой `stop-pipeline`.{% if audience == "internal" %} Или через [UI](../../yandex-specific/flow/release/ui.md).{% endif %}
      "paused":
        description: >-
          Пайплайн приостановлен. Все джобы остановлены, но промежуточные сообщения в пайплайне могут быть не обработаны.

          Запустить запуск перехода в это состояние можно командой `pause-pipeline`.{% if audience == "internal" %} Или через [UI](../../yandex-specific/flow/release/ui.md).{% endif %}
      "working":
        description: >-
          Пайплайн работает. Сообщения обрабатываются.

          Запустить запуск перехода в это состояние можно командой `start-pipeline`.{% if audience == "internal" %} Или через [UI](../../yandex-specific/flow/release/ui.md).{% endif %}
      "draining":
        description: >-
          Промежуточное состояние. Пайплайн в процессе остановки (переход в состояние `stopped`). Дообрабатываются все сообщения, что уже были просмотрены пайплайном из источников и все внутренние  промежуточные сообщения между компьютейшенами.
      "pausing":
        description: >-
          Промежуточное состояние. Пайплайн в процессе приостановки. Все джобы останавливаются.
      "completed":
        description: >-
          Финальное состояние. Пайплайн завершен. Все источники были конечными (`finite`) и все сообщения из них обработаны.

          Выйти из этого состояния сейчас нельзя, только пересоздавать пайплайн.


  "NYT::NFlow::EFetchType":
    values:
      "select_rows":
        description: >-
          Поддерживает динамические (в том числе реплицированные) таблицы, не поддерживает read-ahead, запросы идут сразу к таблетным нодам (рекомендуемый вариант).
      "table_reader":
        description: >-
          Поддерживает динамические и статические таблицы, поддерживает read-ahead, запросы идут через мастер-ноду.

  "NYT::NFlow::EFlowStateTarget":
    values:
      "all":
        description: >-
          Все категории стейтов (по умолчанию).
      "key_state":
        description: >-
          Только `key_states`.
      "partition_state":
        description: >-
          Только `partition_states`.
      "external_key_state":
        description: >-
          Только external-стейты (`external_key_states` и, для `read-states`, `joined_external_key_states`).


  "NYT::NFlow::EDistributionOrdering":
    values:
      "strict":
        description: >-
          Выходные сообщения (созданные из входных сообщений с одинаковым ключом) распределяются в порядке их создания. Этот порядок переживает репартиционирование (невозможна гонка между останавливающимися и работающими партициями, раздающими исходящие сообщения, порожденные из входящих сообщений с одним ключом).
      "relaxed":
        description: >-
          Допускается гонка между останавливающимися и работающими партициями. Немного уменьшает задержки в обработке сообщений при репартиционировании.


  "NYT::NFlow::EUnavailableSourcePolicy":
    values:
      "retry":
        description: >-
          Значение по умолчанию. Ошибка чтения источника роняет итерацию обхода, чтение повторяется; обход не продвигается за непрочитанный диапазон.
      "mark_unreadable":
        description: >-
          Ошибка чтения источника проглатывается, обход идёт дальше, а ключи непрочитанного диапазона резолвятся неинициализированным аксессором (`IsInitialized() == false`).
