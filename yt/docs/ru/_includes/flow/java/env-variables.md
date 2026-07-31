# Переменные окружения в {{product-name}} Flow (Java)

Java companion поддерживает набор переменных окружения для управления поведением приложения и JVM. Переменные окружения необходимо задавать для **worker**-процесса.

## Общие переменные {#general}

#|
|| **Переменная** | **Описание** | **По умолчанию** ||
|| `YT_FLOW_COMPANION_JOB_TTL` | TTL для кеша Job, формат {number}{unit}. | `10m` ||
|#

## Переменные для диагностики и JVM {#diagnostics}

Подробнее о JVM-опциях по умолчанию, профилировании и настройке см. в разделе [Профилирование Java companion](../../../flow/java/profiling.md).

#|
|| **Переменная** | **Описание** | **По умолчанию** ||
|| `YT_FLOW_COMPANION_LOG_DIR` | Директория для логов companion и диагностических файлов JVM. | `./logs` ||
|| `YT_FLOW_COMPANION_JFR_DISABLED` | Если установлена в `1`, отключает опции Java Flight Recorder. | JFR включён ||
|| `YT_FLOW_COMPANION_JFR_OPTS` | Пользовательские JFR-опции через пробел, заменяющие значения по умолчанию. Игнорируется, если задана `YT_FLOW_COMPANION_JFR_DISABLED`. | [см. profiling](../../../flow/java/profiling.md#jfr-defaults) ||
|| `YT_FLOW_COMPANION_GC_LOG_DISABLED` | Если установлена в `1`, отключает логирование GC. | GC-логирование включено ||
|| `YT_FLOW_COMPANION_GC_LOG_OPTS` | Пользовательские опции логирования GC через пробел, заменяющие значения по умолчанию. Игнорируется, если задана `YT_FLOW_COMPANION_GC_LOG_DISABLED`. | [см. profiling](../../../flow/java/profiling.md#gc-defaults) ||
|| `YT_FLOW_COMPANION_JVM_EXTRA_OPTS` | Дополнительные JVM-опции, добавляемые после всех опций по умолчанию. | — ||
|#
