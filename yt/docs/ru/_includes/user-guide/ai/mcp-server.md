---
title: "Работа с MCP-сервером | {{product-name}}"
description: "Как использовать MCP-сервер {{product-name}}: примеры промптов для AI-ассистента и полный справочник доступных инструментов"
---

# Работа с MCP-сервером {{product-name}}

MCP-сервер {{product-name}} — это посредник между AI-ассистентом и кластерами {{product-name}}. Он предоставляет нейросети набор инструментов для чтения данных и метаданных. Ассистент обращается к кластеру напрямую и работает с его актуальным состоянием, а не генерирует ответы по памяти.

## Принцип работы {#how-it-works}

Вы ставите задачу на естественном языке, а ассистент сам выбирает и комбинирует нужные инструменты. Например, чтобы разобраться, почему не удаётся записать данные в таблицу, ассистент последовательно проверит существование пути, права пользователя и квоту аккаунта, а затем объяснит причину.

С помощью MCP-сервера нейросеть может:

- читать данные и метаданные — схему таблицы, примеры строк, содержимое директории;
- проверять права доступа — какие права есть у пользователя на объект;
- отслеживать квоты и ресурсы — свободное место на HDD и SSD, лимиты и текущую загрузку пулов;
- искать объекты — таблицы, файлы и другие узлы по имени и атрибутам.

Формат ответа можно уточнить прямо в промпте.

## Установка {#installation}

Установка и настройка сервера описана в разделе [Установка MCP-сервера](../../../admin-guide/install-mcp.md).

## Примеры промптов {#examples}

Анализ данных и структуры таблиц

:   - Посмотри структуру таблицы `//home/team/users` на кластере `my_cluster` и напиши Python-скрипт, который читает из неё колонку `user_id`.
    - Выведи пример данных из таблицы `//home/team/logs/today`.
    - В папке `//home/team/data` лежат таблицы. Определи их схему.

Управление правами и поиск объектов

:   - Почему мой скрипт падает с ошибкой доступа при записи в `//home/team/output`? Проверь права пользователя `ivanov`.
    - Найди все таблицы с префиксом `backup_` в директории `//home/team` и скажи, кто их владелец.

Работа с квотами и ресурсами

:   - Сколько места на SSD осталось у аккаунта `my_account`?
    - Посмотри лимиты пула `compute_pool` на кластере `my_cluster` — может, упёрлись в квоту по CPU?

{% note tip %}

Если ассистент говорит, что не может выполнить задачу, направьте его, явно указав метод из справочника ниже. Например: «Используй инструмент `check_is_paths_exist`, чтобы убедиться, что папка существует».

{% endnote %}

## Справочник инструментов {#methods-reference}

В этом разделе описаны все инструменты, которые доступны AI-ассистенту. Вы можете ссылаться на их названия в своих промптах. Ниже — сводка по группам. Нажмите на имя инструмента, чтобы перейти к его описанию.

### Навигация и поиск объектов {#nav-tools}

#|
|| **Инструмент** | **Назначение** ||
|| [list_dir](#list-dir) | Возвращает содержимое узла или каталога ||
|| [find](#find) | Ищет объекты в поддереве кластера ||
|| [check_is_paths_exist](#check-is-paths-exist) | Проверяет существование путей ||
|#

### Таблицы: данные и схемы {#table-tools}

#|
|| **Инструмент** | **Назначение** ||
|| [common_client_read_table](#common-client-read-table) | Читает строки таблицы ||
|| [common_client_sample_static_table](#common-client-sample-static-table) | Возвращает первую строку таблицы для быстрого просмотра ||
|| [common_client_get_table_schema](#common-client-get-table-schema) | Возвращает схему таблицы ||
|| [common_client_infer_table_schema](#common-client-infer-table-schema) | Выводит схему из содержимого таблицы ||
|#

{% note info %}

Инструменты `common_client_read_table`, `common_client_sample_static_table` и `common_client_infer_table_schema` читают строки таблицы через HTTP-прокси кластера. Для их работы хосту, где запущен MCP-сервер, нужен сетевой доступ к HTTP-прокси. Как настроить внешний доступ к прокси — в разделе [Настройка внешнего доступа к кластеру](../../../admin-guide/cluster-access-proxy/index.md). Инструменты чтения метаданных, например `common_client_get_table_schema` и `list_dir`, работают через мастер-сервер и такого доступа не требуют.

{% endnote %}

### Права доступа {#access-tools}

#|
|| **Инструмент** | **Назначение** ||
|| [check_permission](#check-permission) | Проверяет права пользователя на путь ||
|| [common_client_whoami](#common-client-whoami) | Возвращает информацию о текущем пользователе ||
|#

### Квоты и ресурсы {#quota-tools}

#|
|| **Инструмент** | **Назначение** ||
|| [get_attributes_account](#get-attributes-account) | Возвращает атрибуты аккаунта ||
|| [get_attributes_account_limits_disk](#get-attributes-account-limits-disk) | Возвращает дисковую квоту и занятое место ||
|| [get_attributes_bundle](#get-attributes-bundle) | Возвращает атрибуты бандла: лимиты и квоты ресурсов ||
|| [get_attributes_pool](#get-attributes-pool) | Возвращает атрибуты и текущую загрузку пула ||
|| [get_account_property](#get-account-property) | Возвращает свойство аккаунта, например дерево дочерних ||
|#

### Инфраструктура {#infra-tools}

#|
|| **Инструмент** | **Назначение** ||
|| [get_proxy](#get-proxy) | Возвращает список прокси-серверов кластера ||
|#

### list_dir {#list-dir}

Возвращает содержимое узла или каталога в кластере {{product-name}}.

Результат содержит метаданные каждого узла: тип `file`, `table` или `map_node`, аккаунт, время создания, количество строк.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `directory` | string | Да | Путь к узлу или каталогу. Должен начинаться с `//` ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|#

Пример запроса:

```json
{
  "directory": "//home/team",
  "cluster": "my_cluster"
}
```

### find {#find}

Ищет объекты в поддереве кластера.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `root_path` | string | Да | Корневой путь для начала поиска. Должен начинаться с `//` ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|| `name` | string | Нет | Шаблон имени в shell-стиле ||
|| `type` | array[string] | Нет | Типы объектов: `table`, `file`, `document`, `account`, `user`, `list_node`, `map_node` ||
|| `attributes` | array[string] | Нет | Атрибуты, которые нужно включить в результат, например `account`, `owner` ||
|| `attributes_to_match` | object | Нет | Фильтрация по значениям атрибутов, например по `owner` или `account` ||
|#

Пример запроса:

```json
{
  "root_path": "//home/team",
  "cluster": "my_cluster",
  "name": "log_*",
  "type": ["table"],
  "attributes": ["owner"],
  "attributes_to_match": {
    "owner": "ivanov"
  }
}
```

### check_is_paths_exist {#check-is-paths-exist}

Проверяет наличие путей в кластере {{product-name}}.

Список может содержать от 1 до 500 путей. Каждый путь должен начинаться с `//` и не заканчиваться `/`.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|| `paths` | array[string] | Да | Список путей до 500 штук. Каждый путь должен начинаться с `//` и не заканчиваться `/` ||
|#

Пример запроса:

```json
{
  "paths": ["//home/team/data", "//tmp/temp_table"],
  "cluster": "my_cluster"
}
```

### common_client_read_table {#common-client-read-table}

Читает строки таблицы.

{% note warning "Внимание" %}

Данные могут быть большими и превысить контекстное окно модели. Для быстрого просмотра структуры используйте [`common_client_sample_static_table`](#common-client-sample-static-table).

{% endnote %}

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `table` | string | Да | Путь к таблице ||
|| `method` | string | Да | Должно быть `read_table` ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|#

Пример запроса:

```json
{
  "table": "//home/team/users",
  "method": "read_table",
  "cluster": "my_cluster"
}
```

### common_client_sample_static_table {#common-client-sample-static-table}

Возвращает первую строку статической таблицы. Удобен для быстрого ознакомления с данными без полной загрузки таблицы.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `table` | string | Да | Путь к таблице с селектором строк, например `//home/team/users[#0:#1]` — первая строка. Для нескольких строк укажите диапазон `[#0:#N]` ||
|| `method` | string | Да | Должно быть `read_table` ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|#

Пример запроса:

```json
{
  "table": "//home/team/users[#0:#1]",
  "method": "read_table",
  "cluster": "my_cluster"
}
```

### common_client_get_table_schema {#common-client-get-table-schema}

Возвращает схему таблицы. Схема хранится в поле `value`, поле `attributes` содержит флаги `strict` и `unique_keys`.

{% note info %}

Если возвращается пустая схема, воспользуйтесь методом [`common_client_infer_table_schema`](#common-client-infer-table-schema), который выводит схему из содержимого таблицы.

{% endnote %}

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `table_path` | string | Да | Путь к таблице ||
|| `method` | string | Да | Должно быть `get_table_schema` ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|#

Пример запроса:

```json
{
  "table_path": "//home/team/users",
  "method": "get_table_schema",
  "cluster": "my_cluster"
}
```

### common_client_infer_table_schema {#common-client-infer-table-schema}

Определяет схему таблицы по её содержимому. Используйте этот метод, если [`common_client_get_table_schema`](#common-client-get-table-schema) вернул пустую схему.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `table` | string | Да | Путь к таблице ||
|| `method` | string | Да | Должно быть `infer_table_schema` ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|#

Пример запроса:

```json
{
  "table": "//home/team/users",
  "method": "infer_table_schema",
  "cluster": "my_cluster"
}
```

### check_permission {#check-permission}

Проверяет право пользователя на доступ к указанному пути. Ответ содержит поле `action` с результатом проверки: `allow` — доступ разрешён, `deny` — доступ запрещён.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `path` | string | Да | Путь к объекту ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|| `permission` | string | Да | Право: `read`, `write`, `use`, `create`, `administer` ||
|| `user_login` | string | Да | Логин пользователя ||
|#

Пример запроса:

```json
{
  "path": "//home/team",
  "cluster": "my_cluster",
  "permission": "read",
  "user_login": "user_login"
}
```

Пример результата:

```json
{
  "action": "allow"
}
```

### common_client_whoami {#common-client-whoami}

Возвращает информацию о текущем пользователе на кластере.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `method` | string | Да | Должно быть `get_current_user` ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|#

Пример запроса:

```json
{
  "method": "get_current_user",
  "cluster": "my_cluster"
}
```

### get_attributes_account {#get-attributes-account}

Возвращает атрибуты учётной записи на кластере.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `account` | string | Да | Имя аккаунта. Без пробелов ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|| `attributes` | array[string] | Да | Атрибуты, например `inherit_acl`, `effective_acl`, `abc`, `resource_limits`, `resource_usage` ||
|#

Пример запроса:

```json
{
  "account": "my_account",
  "cluster": "my_cluster",
  "attributes": ["resource_usage", "effective_acl"]
}
```

### get_attributes_account_limits_disk {#get-attributes-account-limits-disk}

Возвращает дисковую квоту аккаунта и объём занятого места на HDD и SSD.

Значения возвращаются в байтах:

- `resource_limits.disk_space_per_medium.default` — квота аккаунта на HDD;
- `resource_limits.disk_space_per_medium.ssd_blobs` — квота аккаунта на SSD;
- `resource_usage.disk_space_per_medium.default` — занятое место на HDD;
- `resource_usage.disk_space_per_medium.ssd_blobs` — занятое место на SSD.

Другие ресурсы, такие как количество узлов, таблетов и статическая память, не учитываются.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `account` | string | Да | Имя аккаунта ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|| `attributes` | array[string] | Да | Должны быть одновременно указаны `resource_usage` и `resource_limits` ||
|#

Пример запроса:

```json
{
  "account": "my_account",
  "cluster": "my_cluster",
  "attributes": ["resource_usage", "resource_limits"]
}
```

### get_attributes_bundle {#get-attributes-bundle}

Возвращает значения атрибутов бандла на кластере: ограничения ресурсов с детализацией по количеству таблетов и статической памяти, а также квоты по CPU и памяти.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|| `attributes` | array[string] | Да | Список запрашиваемых атрибутов, например `inherit_acl`, `effective_acl`, `resource_limits`, `resource_quota` ||
|| `bundle` | string | Нет | Имя бандла. Без пробелов ||
|#

Пример запроса:

```json
{
  "bundle": "my_bundle",
  "cluster": "my_cluster",
  "attributes": ["resource_limits", "resource_quota"]
}
```

### get_attributes_pool {#get-attributes-pool}

Возвращает атрибуты пула на кластере.

{% note info %}

Пул ищется в дереве пулов, указанном в параметре `pool_tree`. Значение по умолчанию `physical` подходит не для всех кластеров — имя дерева зависит от конфигурации. Если пул не найден, уточните имя дерева у администратора кластера.

{% endnote %}

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|| `attributes` | array[string] | Да | Атрибуты, например `strong_guarantee_resources`, `integral_guaranties`, `max_operation_count`, `max_running_operation_count`, `running_operation_count`, `scheduling_status`, `starvation_status`, `resource_usage`. Атрибут `resource_usage` показывает текущую загрузку пула: CPU, GPU, память, слоты ||
|| `pool` | string | Нет | Имя пула. Уникально в пределах дерева. Без пробелов ||
|| `pool_tree` | string | Нет | Имя дерева пулов. По умолчанию `physical` ||
|#

Пример запроса:

```json
{
  "pool": "pool_name",
  "pool_tree": "pool_tree",
  "cluster": "my_cluster",
  "attributes": ["max_operation_count", "effective_acl"]
}
```

### get_account_property {#get-account-property}

Возвращает свойство аккаунта.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `account` | string | Да | Имя аккаунта ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|| `property` | string | Да | Свойство аккаунта. Например, `childrens` возвращает дерево дочерних аккаунтов ||
|#

Пример запроса:

```json
{
  "account": "my_account",
  "cluster": "my_cluster",
  "property": "childrens"
}
```

### get_proxy {#get-proxy}

Возвращает список прокси-серверов кластера с указанными атрибутами.

Параметры:

#|
|| **Параметр** | **Тип** | **Обязательный** | **Описание** ||
|| `cluster` | string | Да | Название кластера {{product-name}} ||
|| `attributes` | array[string] | Да | Атрибуты прокси-сервера: `proxy_type`, `type`, `role`, `version` ||
|| `proxies` | array[string] | Нет | Список прокси-серверов в формате `fqdn:port`. Если не указан, применяется ко всем прокси кластера ||
|| `proxy_type` | string | Нет | Тип прокси: `http` или `rpc`. Если не указан, возвращаются все типы ||
|#

Пример запроса:

```json
{
  "cluster": "my_cluster",
  "attributes": ["role", "version"],
  "proxy_type": "http"
}
```


<style>
  .dc-mini-toc__section_child {
    display: none;
}

@media screen and (max-width: 768px) {
    .dc-doc-page__content-mini-toc ul li ul {
        display: none;
    }
}
</style>
