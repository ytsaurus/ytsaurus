## Query tracker


Поставляется в виде docker-образа.




**Релизы:**

{% cut "**0.1.2**" %}

**Дата релиза:** 2026-01-27


**Страница релиза:** [0.1.2](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.1.2)


**Docker-образ:** [ghcr.io/ytsaurus/query-tracker:stable-0.1.2](https://github.com/orgs/ytsaurus/packages/container/query-tracker/659176566?tag=stable-0.1.2)


**Возможности**
- [экспериментально] Добавлена возможность обработки YQL-запросов в отдельных процессах

**Исправления**
- Исправлен список версий языка в UI


**NB!** Доступно только с версией прокси [25.2.2](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fytsaurus%2F25.2.2) и новее, UI [3.3.1](https://github.com/ytsaurus/ytsaurus-ui/releases/tag/ui-v3.3.1) и новее.


{% endcut %}


{% cut "**0.1.1**" %}

**Дата релиза:** 2025-12-18


**Страница релиза:** [0.1.1](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.1.1)


**Docker-образ:** [ghcr.io/ytsaurus/query-tracker:0.1.1](https://github.com/orgs/ytsaurus/packages/container/query-tracker/617510568?tag=0.1.1)


Это альфа-релиз, обновитесь до [0.1.2](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fquery-tracker%2F0.1.2) или новее

**Возможности**
- Поддержка версионирования языка YQL
- Поддержка Spark Connect (SPYT Connect)
- Новые возможности обработки партиционированных таблиц в YQL

**Исправления**
- Исправлено выполнение больших запросов за счёт сжатия колонки 'progress'
- Исправлена возможная блокировка сетевого сокета (между QT и YQL-агентом) при прерывании YQL-запроса

**Известные баги**
- Некорректные версии языка в UI

**NB!** Доступно только с версией прокси [25.2.2](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fytsaurus%2F25.2.2) и новее

{% endcut %}


{% cut "**0.0.11**" %}

**Дата релиза:** 2025-09-08


**Страница релиза:** [0.0.11](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.0.11)


**Docker-образ:** [ghcr.io/ytsaurus/query-tracker:0.0.11](https://github.com/orgs/ytsaurus/packages/container/query-tracker/509269551?tag=0.0.11)


**Возможности**
- Добавлен флаг "sort_order" для API list_queries
- Добавлена мета "assigned_engine" для YQL-запросов
- Таймаут YQL-запросов стал настраиваемым
- Поддержка версий языка YQL в cli\sdk QT.
https://ytsaurus.tech/docs/en/yql/changelog/#general-description-of-yql-versions
- Поддержка возврата AST YQL-запросов в get_query cli\sdk

**Улучшения**
- Улучшена механика дополнительных секретов в YQL-запросах

**Исправления**
- Исправлено чтение результатов YQL с yson-полями по ссылке

**NB!** Большинство новых возможностей доступно только с версией прокси 24.2\25.1 и новее

{% endcut %}


{% cut "**0.0.10**" %}

**Дата релиза:** 2025-06-23


**Страница релиза:** [0.0.10](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.0.10)


**Docker-образ:** [ghcr.io/ytsaurus/query-tracker:0.0.10](https://github.com/orgs/ytsaurus/packages/container/query-tracker/448865204?tag=0.0.10)


**Возможности:**
- Добавлена clickhouse UDF
- Добавлена мета "assigned_tracker" для запросов в состоянии "finished"

**Внутренние изменения:**
- Изменён внутренний формат результатов YQL. Это не должно быть заметно пользователям.

{% endcut %}


{% cut "**0.0.9**" %}

**Дата релиза:** 2025-04-08


**Страница релиза:** [0.0.9](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.0.9)


**Docker-образ:** [ghcr.io/ytsaurus/query-tracker:0.0.9](https://github.com/orgs/ytsaurus/packages/container/query-tracker/393209556?tag=0.0.9)


**Возможности**
- Добавлена возможность бана QT\YQLA
- Добавлены пользовательские метрики QT\YQLA
- Добавлена динамическая конфигурация YQLA
- Добавлена возможность указания дополнительных учётных данных для YQL-запросов
- Добавлена возможность настройки кластера YQL по умолчанию для каждого запроса
- Добавлена полная таблица результатов в результатах YQL-запросов (доступно с прокси 25.1)
- Поддержка символов Unicode в SPYT-запросах
- Поддержка обрезания результатов в SPYT-запросах

**Улучшения**
- Оптимизированы вызовы QT API
- Ограничено максимальное количество одновременных запросов на YQLA
- Настроена компактификация dyntables QT

**Исправления**
- Исправлено прерывание YQL-запросов
- Исправлено завершение запросов с результатами более 16MB
- Исправлено завершение запроса после падения ответственного qt
- Исправлен дедлок yqla

**NB!** Этот релиз совместим только с версией прокси 24.1.0, версией оператора 0.23.1 и новее
https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fytsaurus%2F24.1.0
https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.23.1

{% endcut %}


{% cut "**0.0.8**" %}

**Дата релиза:** 2024-08-26


**Страница релиза:** [0.0.8](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.0.8)


**Docker-образ:** [ghcr.io/ytsaurus/query-tracker:0.0.8](https://github.com/orgs/ytsaurus/packages/container/query-tracker/264406046?tag=0.0.8)


- Оптимизирована производительность Query Tracker API за счёт добавления индексов системных таблиц. Issue: #653
- Добавлена поддержка SystemPython udfs в YQL-запросах. Issue: #265
- Исправлено сжатие сломанных логов в YQL-агенте. Issue: #623
- Оптимизирована производительность одновременных YQL-запросов
- Исправлена утечка памяти в YQL Agent
- **Важное исправление.** Исправлено повреждение результатов YQL-запросов в DQ. Issue: #707
- Добавлена поддержка DQ в dual stack сетях. Issue: #744

{% endcut %}


{% cut "**0.0.7**" %}

**Дата релиза:** 2024-08-01


**Страница релиза:** [0.0.7](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.0.7)


**Docker-образ:** [ghcr.io/ytsaurus/query-tracker:0.0.7](https://github.com/orgs/ytsaurus/packages/container/query-tracker/252093623?tag=0.0.7)


- **Важное исправление.** Исправлено повреждение результатов YQL-запросов. Issue: https://github.com/ytsaurus/ytsaurus/issues/707
- Исправлен запуск YQL DQ
- Исправлен баг, вызывавший ошибки UTF-8 в логах yql-agent
- Исправлены множественные дедлоки в yql-agent
- Добавлена поддержка SPYT discovery groups
- Добавлена поддержка параметров SPYT-запросов
- Добавлен ACO everyone-share, который можно использовать для обмена запросами по ссылке
- Добавлена поддержка нескольких ACO на запрос, возможность будет доступна в свежих релизах UI, SDK
- Изменено взаимодействие между Query Tracker и прокси

**NB!** Этот релиз совместим только с версией прокси 23.2.1, версией оператора 0.10.0 и новее
https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fytsaurus%2F23.2.1
https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.10.0


{% endcut %}


{% cut "**0.0.6**" %}

**Дата релиза:** 2024-04-11


**Страница релиза:** [0.0.6](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.0.6)


**Docker-образ:** [ghcr.io/ytsaurus/query-tracker:0.0.6](https://github.com/orgs/ytsaurus/packages/container/query-tracker/223408391?tag=0.0.6)


- Исправлена авторизация в сложных cluster-free YQL-запросах
- Исправлен баг, из-за которого запросы с большими запросами никогда не завершались
- Исправлен баг, вызывавший возможность SQL-инъекции в query tracker
- Уменьшен размер docker-образов query_tracker

**Связанные issues:**
- [Проблемы с QT ACOs](https://github.com/ytsaurus/yt-k8s-operator/issues/176)

В случае ошибки при запуске запроса
```
Access control object "nobody" does not exist
```
Необходимо выполнить команды от администратора
```
yt create access_control_object_namespace --attr '{name=queries}'
yt create access_control_object --attr '{namespace=queries;name=nobody}'
```



{% endcut %}

{% cut "**0.0.5**" %}

**Дата релиза:** 2024-03-19


**Страница релиза:** [0.0.5](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.0.5)


- Добавлен контроль доступа к запросам
- Добавлена поддержка in‑memory DQ engine, который ускоряет выполнение небольших YQL‑запросов
- Добавлена настройка режима выполнения в query tracker. Это позволяет запускать запросы в режимах validate и explain
- Исправлена ошибка, из‑за которой запросы терялись в query_tracker
- Исправлена ошибка, связанная с разбором yson в YQL‑запросах
- Снижена нагрузка на динамические таблицы состояния со стороны QT
- Улучшена аутентификация в YQL‑запросах
- Добавлена аутентификация в SPYT‑запросах
- Добавлено повторное использование spyt‑сессий. Ускоряет последовательный запуск SPYT‑запросов от одного пользователя
- Изменён тип сборки образов QT с cmake на ya make

**NB:**
- Совместимо только с версией оператора [0.6.0](https://github.com/ytsaurus/yt-k8s-operator/releases/tag/release%2F0.6.0) и новее
- Совместимо только с версией прокси [23.2](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fytsaurus%2F23.2.0) и новее
- Перед обновлением ознакомьтесь с разделом [документации](https://ytsaurus.tech/docs/ru/user-guide/query-tracker#access-control), содержащим информацию о новом контроле доступа к запросам

**Новые связанные проблемы:**
- [Проблемы с ACO в QT](https://github.com/ytsaurus/yt-k8s-operator/issues/176)

При возникновении ошибки при запуске запроса
```
Access control object "nobody" does not exist
```
Необходимо выполнить команды от имени администратора
```
yt create access_control_object_namespace --attr '{name=queries}'
yt create access_control_object --attr '{namespace=queries;name=nobody}'
```



{% endcut %}


{% cut "**0.0.4**" %}

**Дата релиза:** 2023-12-03


**Страница релиза:** [0.0.4](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.0.4)


- Применены значения YQL по умолчанию из документации
- Исправлена ошибка в YQL‑запросах, не использующих таблицы YT
- Исправлена ошибка в YQL‑запросах, использующих агрегатные функции
- Добавлена поддержка распространённых UDF‑функций в YQL

NB: Этот релиз совместим только с оператором 0.5.0 и более новыми версиями.
https://github.com/ytsaurus/yt-k8s-operator/releases/tag/release%2F0.5.0



{% endcut %}


{% cut "**0.0.3**" %}

**Дата релиза:** 2023-11-14


**Страница релиза:** [0.0.3](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.0.3)


- Исправлена ошибка, из‑за которой транзакция пользователя истекала до завершения yql‑запроса в сетях только с IPv4.
- Системные таблицы query_tracker перенесены в sys bundle


{% endcut %}


{% cut "**0.0.1**" %}

**Дата релиза:** 2023-10-19


**Страница релиза:** [0.0.1](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/query-tracker/0.0.1)


- Добавлена аутентификация, теперь все запросы выполняются от имени пользователя, который их инициировал.
- Добавлена поддержка типов v3 в YQL‑запросах.
- Добавлена возможность задавать кластер по умолчанию для выполнения YQL‑запросов.
- Изменён формат представления ошибок YQL‑запросов.
- Исправлена ошибка, которая приводила к сбоям при выполнении запросов, не возвращающих результат.
- Исправлена ошибка, которая приводила к сбоям при выполнении запросов, извлекающих данные из динамических таблиц.
- Исправлена ошибка, которая приводила к ошибкам использования памяти. YqlAgent больше не падает без причины под нагрузкой.


{% endcut %}
