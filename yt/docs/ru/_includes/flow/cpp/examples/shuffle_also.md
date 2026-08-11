## См. также

- [Быстрый старт (C++)](../../../../flow/cpp/getting-started.md)
- [Computation (C++)](../../../../flow/cpp/computation.md)

- Для группировки по `value` обязательно необходимо указать эту колонку (и хэш от неё) в `group_by_schema`.
- В `input_stream_ids` перечисляются все потоки: `event_a`, `event_b`, `event_c`, `event_d` &mdash; чтобы читать все получившиеся потоки. С точки зрения "бизнес логики" это не самое осмысленное действие, однако исходной целью данного пайплайна было протестировать гарантии `exactly-once` даже в случае `Swift` цепочки.
- Так как `TReducer` является наследником `TTransformComputation` &mdash; то `input_message_ids` и `output_messages` в обязательном порядке сохраняются в {{product-name}}. Но `output_messages` у нас пустые. По сути, данный пайплайн сохраняет в {{product-name}} только метаинформацию в рамках `reader`, метаинформацию (`message_id` и `key`) на каждое входное сообщение и таблицу `value => count` в рамках `reducer`. Промежуточные `computation` вообще не взаимодействуют с {{product-name}}.

### DynamicSpec

- Поле `dynamic_spec/computations/<computation_id>/desired_partition_count` заполняется для каждого `computation`, кроме `reader`. В рамках теста `test_shuffle.py` происходит изменение числа партиций.
- В `dynamic_spec/job_tracker/job_threads` указывается необходимое число тредов для выполнения всех джобов.

### Config для запуска

- Ключевое для запуска: `cluster_url`, `proxy_role`, `path`, `rpc_proxy`, `monitoring_port`.
- `controller/scheduler_period` выставлен в 200 для конкретного теста - в реальности должно быть достаточно дефолтного значения.
- `logging` - настройки логирования.

```yson
{
    "cluster_url" = "cluster_name";
    "path" = "//path/to/pipeline";
    "rpc_port" = 81;
    "monitoring_port" = 80;
    "controller" = {
        "scheduler_period" = 200;
    };
    "logging" = {
        "suppressed_messages" = [
        ];
        "rules" = [
            {
                "exclude_categories" = [
                    "Bus";
                    "Dns";
                    "Concurrency";
                    "QueryClient";
                    "Profiling";
                    "RpcClient";
                    "Monitoring";
                    "Net";
                    "Solomon";
                    "Jaeger";
                    "RpcProxyClient";
                    "RpcServer";
                    "Dns";
                    "BufferMetrics";
                ];
                "min_level" = "debug";
                "writers" = [
                    "Stderr";
                ];
            };
        ];
        "writers" = {
            "Stderr" = {
                "type" = "file";
                "file_name" = "/path/to/file.log";
            };
        };
    }
}
```

