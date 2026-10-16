# Профилирование и бектрейсы {{product-name}} Flow

Снимайте диагностические данные с живого процесса контроллера или воркера, когда логи показывают зависание, высокую загрузку CPU или рост памяти. Сначала войдите в [job shell](vanilla/diagnostics/logs.md#logs) нужной джобы. Если {{product-name}} выделил порт мониторинга, используйте `YT_PORT_1`; иначе используйте настроенный `monitoring_port`, по умолчанию `10081`. Если вы задали другое значение, перед запуском команд ниже присвойте его переменной `FLOW_CONFIGURED_MONITORING_PORT`. Сетевой проект или `port_count = 0` не гарантируют фиксированный порт воркера, когда компаньонский раннер на Go или Python увеличивает `worker.port_count`. Подробнее — в [параметрах портов Vanilla](vanilla/initial-deploy.md#advanced-config).

```bash
FLOW_MONITORING_PORT="${YT_PORT_1:-${FLOW_CONFIGURED_MONITORING_PORT:-10081}}"
```

## Бектрейсы {#backtraces}

```bash
curl -fsS "http://localhost:${FLOW_MONITORING_PORT}/backtrace/threads" > /tmp/flow-threads.txt
curl -fsS "http://localhost:${FLOW_MONITORING_PORT}/backtrace/fibers" > /tmp/flow-fibers.txt
```

Сравните стеки повторных снимков: одинаковая точка ожидания помогает локализовать зависание. Сохраните файлы вместе с временем снимка и ID джобы. Если ответ обрезан, при наличии права `ptrace` и установленного GDB снимите стеки всех тредов через `gdb -batch -ex 'thread apply all bt' -p <pid>`; подключение кратковременно останавливает процесс, поэтому делайте это на выбранной джобе.

## Orchid и память {#orchid}

```bash
curl -fsS "http://localhost:${FLOW_MONITORING_PORT}/orchid/monitoring/ref_counted" > /tmp/flow-ref-counted.json
curl -fsS "http://localhost:${FLOW_MONITORING_PORT}/orchid/job_tracker" > /tmp/flow-jobs.json
```

Первый ответ содержит статистику живых объектов с подсчётом ссылок, второй — сведения воркера о джобах. Сравните несколько снимков при одинаковой нагрузке, прежде чем считать рост утечкой. Если сборка поддерживает heap-профиль, запросите `http://localhost:${FLOW_MONITORING_PORT}/ytprof/heap`; для анализа нужны совместимые символы бинаря и инструмент чтения профиля.

## CPU {#cpu}

При доступном встроенном профилировщике запросите `http://localhost:${FLOW_MONITORING_PORT}/ytprof/profile?d=30s` и сохраните результат для просмотра профиля. Если ручка отсутствует, используйте доступный в окружении `perf` на PID процесса и сопоставьте профиль с логами и состоянием партиций. Профилирование создаёт нагрузку; измеряйте короткий интервал и не принимайте решение о масштабировании по одному снимку.

Для исключений с неясным происхождением можно временно включить `singletons.error_backtrace_enricher.level = "enabled_for_all"` в динамической спеке. Отключите настройку после расследования: символизация бектрейсов может задерживать обработку.
