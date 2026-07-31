# Distributed Throttler в {{product-name}} Flow

Распределённый троттлер — глобальный для всего [пайплайна](../../../flow/concepts/glossary.md#pipeline) механизм ограничения скорости. На контроллере для каждого именованного троттлера держится [token bucket](https://en.wikipedia.org/wiki/Token_bucket); [джобы](../../../flow/concepts/glossary.md#job) запрашивают у контроллера квоту перед обработкой сообщений или перед произвольным пользовательским действием.

Квотироваться может либо число обработанных сообщений, либо суммарный размер их payload в байтах — отдельно или вместе.

Типичные задачи:

- ограничить суммарную нагрузку на внешний API со стороны всех джобов;
- равномерно распределить пропускную способность между [партициями](../../../flow/concepts/glossary.md#partition) одного [`Computation`](../../../flow/concepts/computation.md);
- притормозить чтение из источника.

## Приоритет по отставанию {#priority}

Каждая джоба прикладывает к запросу за квотой свою временную метку — ту же, по которой во входном буфере определяется отставание. Берётся минимум по всем `stabilized_event_timestamp + stream_delay` из [input streams](../../../flow/concepts/glossary.md#stream) и `read_alignment_timestamp` из [source streams](../../../flow/concepts/glossary.md#source). Сервер ранжирует запросы по возрастанию этой метки: сначала отдаёт квоту тем, кто отстаёт сильнее. При перекосе нагрузки между [партициями](../../../flow/concepts/glossary.md#partition) одного [`Computation`](../../../flow/concepts/computation.md) отстающие автоматически получают больше квоты, а лидирующие — замедляются.

## Реконфигурация на лету {#reconfigure}

Все параметры троттлера (`limit`, `period`, `request_period`, `retrying_channel`, `rpc_timeout`) подхватываются без перезапуска пайплайна — достаточно обновить `dynamic_spec`. Кэшированный в пользовательском коде `IThroughputThrottlerPtr` остаётся валидным после смены конфига.

## Что будет, если контроллер недоступен {#controller-unavailable}

Клиент троттлера переоткрывает соединение через `retrying_channel`. Пока контроллер не отвечает, локальный prefetch-кэш доедается, и `Throttle()` блокируется в ожидании квоты — джоба прогресса не делает. Когда контроллер возвращается, ожидание разрешается само. Если же контроллер не отвечает дольше, чем суммарный бюджет ретраев `retrying_channel`, работа с троттлером выкинет исключение и джоба упадёт.

## Что будет, если квоты не хватает {#quota-insufficient}

Если суммарный спрос джоб на много порядков превышает возможную выдачу, отдельные `RequestQuota` могут долго стоять в серверной очереди и в итоге не уложиться в свой `rpc_timeout` × `retry_attempts`. В таком случае `Throttle()` тоже выкинет исключение, что приведет к падению джобы.

Fallback на локальный лимит ни в одном из этих случаев сейчас не поддержан.

## Конфигурация {#configuration}

Все настройки — в `DynamicSpec`. Троттлеры объявляются в корне. В примере ниже `external_api_quota` — пользовательский id троттлера, `RequestEnricher` — имя computation:

```yson
"dynamic_spec" = {
    "throttlers" = {
        "external_api_quota" = {
            "limit" = 100.0;
        };
    };
    "computations" = {
        "RequestEnricher" = {
            "input_rows_throttler_id" = "external_api_quota";
        };
    };
}
```

Поля `TDynamicThrottlerSpec`:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TDynamicThrottlerSpec.md) %}

## Применение {#usage}

Есть два варианта — автоматический и ручной. Они не конфликтуют: можно пользоваться одним или сразу обоими.

### Автоматический: ограничить скорость входного батча {#auto}

В `TDynamicComputationSpec` есть два поля — `input_rows_throttler_id` и `input_bytes_throttler_id`. Если они заданы, перед каждой итерацией `Process` компьютейшен ждёт квоту:

- по числу сообщений во входном батче (`input_rows_throttler_id`);
- по сумме `byte_size` сообщений батча — это системный размер сериализованного представления, без учёта компрессии/шифрования на сетевом уровне (`input_bytes_throttler_id`).

Ожидание оформляется отдельным span'ом `Input.Throttle` в трейсинге компьютейшена (видно в Jaeger и на графиках "Epoch parts time" в UI).

Id должен быть объявлен в `dynamic_spec/throttlers`.

### Ручной: `GetThrottler(id)` из пользовательского кода {#manual}

Если автоматического троттлинга по входному батчу недостаточно (например, нужен rate-limit на каждый внешний запрос), троттлер берут напрямую из пользовательского `Computation`. У базового класса есть метод `GetThrottler(id)`, который возвращает `IThroughputThrottlerPtr` — стандартный YT-троттлер, у которого вызывают `Throttle(amount)` и ждут результат.

Возвращённый указатель стабилен в рамках всей жизни джоба: фабрика подменяет внутренний клиент при `Reconfigure`, поэтому пользователь может сохранить его в `DoInit` и использовать дальше без переполучения.

Сейчас этот механизм поддержан только в C++.

## Мониторинг {#monitoring}

У дашборда Flow есть отдельный таб **Distributed throttler**. Основные сенсоры:

- серверные (контроллер): `value.rate` — фактическая выдача квоты, `queue_size` — сколько запросов ждут, `wait_time.max` — хвост ожидания.
- клиентские (воркер, на каждом `Computation`): `consumed.rate` / `released.rate`, `wait_time.max` — время, которое джоба простояла в `Throttle()`, `request_count.rate` — частота prefetch-RPC.

## См. также {#see-also}

- [Spec, DynamicSpec и Config](../../../flow/concepts/spec.md)
- [Computation](../../../flow/concepts/computation.md)
