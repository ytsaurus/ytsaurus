# Коннектор QYT в {{product-name}} Flow

Коннектор к [очередям {{product-name}}](../../../user-guide/dynamic-tables/queues.md) (QYT).

Код коннектора находится [здесь]({{source-root}}/yt/yt/flow/library/cpp/connectors/queue).

## Чтение из очереди

При чтении из очереди простейшим способом в [`Computation`](../../../flow/concepts/glossary.md#stream-and-computation) приходят сообщения той же схемы, что и у строк таблицы очереди.

По умолчанию `event_time` и `system_time` сообщения считаются на основе колонки `$timestamp`, которая содержит YT-таймстемп записи строки (подробнее в [документации упорядоченных динтаблиц](../../../user-guide/dynamic-tables/queues.md)).
Но при наличии в очереди колонки с метой (смотри параметры статической спеки сорса), `event_time` сообщения может браться из соответствующего поля. Также из меты может браться информация о [ватермарке](../../../flow/concepts/glossary.md#timestamps-and-watermarks) стрима.

Класс [сорса](../../../flow/concepts/glossary.md#source): `NYT::NFlow::TQueueSource`.

Настройки сорса представлены ниже.

{% note info %}

Источник определяется кластером и путём очереди (`queue_path`). При их изменении партиции пересоздаются и читаются заново — см. [Смена источника](../../../flow/connectors/about.md#source-change).

{% endnote %}

### Управление лагом консьюмера

При регистрации консьюмера к очереди лаг для консьюмера устанавливается на последние сохранённые данные в очереди. Таким образом, после регистрации, если политика удаления старых строк на очереди равна 1 дню, лаг будет равняться также одному дню.

Такое поведение может привести к следующему сценарию: при выкатке релиза лаг данных будет равен дню, и обработка сообщений встанет, пока лаг не будет обработан.

Избежать этого можно несколькими способами:

#### 1. Использование стандартной YT CLI

Можно воспользоваться стандартной YT CLI, переставив значение оффсетов у консьюмера на желаемые для каждой партиции. Не очень удобно при первом чтении топика, так как необходимо знать значение самого свежего оффсета, и назначить его для каждой [партиции](../../../flow/concepts/glossary.md#partition).

Пример команды:
```bash
{{yt-cli}} advance-queue-consumer --proxy {{flow-consumer-cluster}} //home/service/stable/consumer {{flow-data-cluster}}://home/source_service/Data/queue --partition-index 0 --new-offset 486482013
```

Расширенные возможности доступны в документации YT и по параметру `-h`.

{% if audience == "internal" %}

#### 2. Использование утилиты bigrtcli

Более удобный способ — воспользоваться утилитой `bigrtcli`.

Пример команды:
```bash
YT_PROXY={{flow-data-cluster}} ya tool bigrtcli consumer update_offsets "<cluster={{flow-consumer-cluster}}>//home/service/prestable/consumer" "<cluster={{flow-data-cluster}};wrapped=%false>//home/source_service/queue" --shards "range(0, 256)" --value "-1"
```

{% note warning "Внимание" %}

Перед выполнением команды обязательно проверьте её корректность, использование параметра `--value "-1"` приведёт к пропуску всех данных во входной очереди в указанных партициях.

{% endnote %}

Здесь мы обновляем оффсеты для консьюмера `//home/service/prestable/consumer` на кластере Pythia, для очереди `//home/source_service/Data` на кластере Markov для всех партиций от 0 до 255, пропуская все данные во входной очереди (по сути, переставляя offset на следующий ещё не занятый для каждой партиции).

{% note info "Важно" %}

Указание `YT_PROXY={{flow-data-cluster}}` необходимо для корректной работы `bigrtcli`, без него произойдёт ошибка. При этом кластер для очереди и консьюмера берутся из самой команды.

{% endnote %}

Больше информации об использовании в [коде]({{source-root}}/bigrt/cli/lib/__init__.py?rev=r18281971#L656) и по параметру `-h`.

{% endif %}

### Статическая спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TQueueSource.md) %}

### Динамическая спека:


{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TQueueSource.md) %}

## Запись в очередь

Для записи сообщений в очередь в [синк](../../../flow/concepts/glossary.md#sink) нужно отправлять сообщения той же схемы, что и у таблицы, в которую планируется запись. Также можно настроить запись метаинформации в специальную колонку, чтобы передавать информацию о `event_time` сообщений и о ватермарке стрима читателям очереди.

Есть два варианта синков: синхронный (`NYT::NFlow::TSyncQueueSink`) и асинхронный (`NYT::NFlow::TAsyncQueueSink`). Запись в синхронный синк идёт в основной транзакции [эпохи](../../../flow/concepts/glossary.md#epoch), это эффективно, но можно писать только в очередь на основном кластере процессинга. Запись в асинхронный синк идёт уже после основной транзакции эпохи из сообщений сохранённых в output messages, это дороже, но так можно писать в очередь на любом кластере.

### Параметры спек синхронного синка

#### Статическая спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TSyncQueueSink.md) %}

#### Динамическая спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TSyncQueueSink.md) %}

### Параметры спек асинхронного синка

#### Статическая спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TUnitedParameters_NYT_NFlow_TAsyncQueueSink.md) %}

#### Динамическая спека:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TAsyncQueueSink.md) %}

{% if audience == "internal" %}

## Расширение для BigRT

Для работы с очередями в BigRT-формате (батчинг и сжатие в одну колонку) используйте расширение [BigRT Queue](../../../yandex-specific/flow/extensions/bigrt.md).

{% endif %}

## См. также

- [Список коннекторов](../../../flow/connectors/about.md)
- [Spec и DynamicSpec](../../../flow/concepts/spec.md)
