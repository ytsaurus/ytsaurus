# Профилирование Java companion в {{product-name}} Flow (Java)

В Java companion включено непрерывное профилирование (continuous profiling) на основе [JDK Flight Recorder](https://wiki.openjdk.org/spaces/jmc/pages/37584926/Overview) (JFR). Профилирование позволяет диагностировать проблемы производительности и анализировать инциденты.

## Возможности {#capabilities}

JFR‑файлы содержат комплексную телеметрию Java‑приложения и JVM. Это позволяет проводить:

* анализ производительности — выявление CPU/RAM‑нагруженных методов и узких мест;
* диагностику проблем с памятью и GC;
* расследование инцидентов постфактум по данным за период сбоя.

JFR отличается низкими накладными расходами и является стандартным инструментом мониторинга Java‑приложений в продакшене.

Полученные JFR‑файлы можно открыть и проанализировать с помощью [Java Mission Control (JMC)](https://wiki.openjdk.org/spaces/jmc/overview) – стандартный инструмент для анализа JFR‑записей.

## Принцип работы {#how-it-works}

Профлирование включено по умолчанию через установленные JVM-опции:

```
-XX:+UnlockDiagnosticVMOptions
-XX:+DebugNonSafepoints
-XX:StartFlightRecording=disk=true,settings=profile,maxage=24h,maxsize=1000m,dumponexit=true,filename=<logDir>/dump.jfr
-XX:FlightRecorderOptions=repository=<logDir>/jfr,maxchunksize=30M
```

JFR записывается на диск чанками по 30 МБ в директорию, заданную переменной окружения `YT_FLOW_COMPANION_LOG_DIR`. Данные хранятся до 24 часов и ограничены общим размером 1 ГБ. Управление JFR осуществляется через [Переменные окружения](../../../flow/java/env-variables.md):

* `YT_FLOW_COMPANION_JFR_DISABLED` — если установлена в `1`, то полностью отключает JFR-опции;
* `YT_FLOW_COMPANION_JFR_OPTS` — пользовательские JFR-опции через пробел, заменяющие значения по умолчанию (игнорируется, если задана `YT_FLOW_COMPANION_JFR_DISABLED`).

## Другие диагностические JVM-опции по умолчанию {#default-jvm-options}

### Логирование GC {#gc-defaults}

Включено по умолчанию:

```
-Xlog:gc:file=<logDir>/gc.log:time,uptime:filecount=10,filesize=50m
```

Где `<logDir>` — значение переменной окружения `YT_FLOW_COMPANION_LOG_DIR`.

Можно переопределить через `YT_FLOW_COMPANION_GC_LOG_OPTS` или отключить через `YT_FLOW_COMPANION_GC_LOG_DISABLED=1`.

### Crash dump и обработка OOM {#crash-defaults}

Всегда включены:

```
-XX:+ExitOnOutOfMemoryError
-XX:+CreateCoredumpOnCrash
-XX:ErrorFile=<logDir>/hs_err_%p.log
```

Где `<logDir>` — значение переменной окружения `YT_FLOW_COMPANION_LOG_DIR`.

### Настройка Heap Dump (рекомендуется) {#heap-dump}

Опции heap dump **не** задаются по умолчанию и должны быть сконфигурированы через переменную окружения `YT_FLOW_COMPANION_JVM_EXTRA_OPTS`:

```
YT_FLOW_COMPANION_JVM_EXTRA_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=<heapDumpDir>"
```

Где `<heapDumpDir>` — путь к директории для хранения heap dump.

{% note warning %}

Heap dump может быть очень большим (вплоть до полного размера heap). Рекомендуется выделить отдельный персистентный volume для heap dump.

{% endnote %}

## Получение данных профилирования {#get-profiling-data}

### Через SSH {#via-ssh}

Подключитесь к worker по SSH и скопируйте на машину разработчика JFR‑файлы из директории заданной в переменной окружения `YT_FLOW_COMPANION_LOG_DIR`.

### Через CLI {#via-cli}

Для выгрузки последнего полного JFR‑чанка с companion выполните команду:

```bash
# <pipeline_path> - путь до директории pipeline в YT.
# <worker> - IP адрес worker с rpc_port, например "[IPv6]:port".

ya run yt/yt/flow/tools/download_jfr --pipeline-path <pipeline_path> --worker <worker>

```

Готовую команду для загрузки JFR с каждого companion можно скопировать в веб-интерфейсе YT на странице пайплайна, вкладка **Workers**.
