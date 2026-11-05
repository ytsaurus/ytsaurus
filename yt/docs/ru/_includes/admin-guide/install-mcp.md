---
title: "Установка MCP-сервера | {{product-name}}"
description: "Как установить пакет ytsaurus-mcp и настроить MCP-сервер {{product-name}} для работы с AI-ассистентом Cline в Visual Studio Code"
---

# Установка MCP-сервера {{product-name}}

В инструкции описано, как установить пакет `ytsaurus-mcp` и настроить MCP-сервер {{product-name}} для работы с AI-ассистентом. В качестве примера используется [Cline](https://marketplace.visualstudio.com/items?itemName=saoudrizwan.claude-dev) в Visual Studio Code. MCP-сервер {{product-name}} работает по открытому стандарту и совместим с любыми MCP-клиентами: Cursor, Windsurf, Claude Desktop, Roo Code и другими. Конфигурация для них будет аналогичной.

О возможностях сервера и доступных инструментах читайте в разделе [Работа с MCP-сервером](../../user-guide/ai/mcp-server.md).

## Пререквизиты {#prerequisites}

Перед началом работы подготовьте:

- Python 3.10 или новее.
- Токен для доступа к кластеру {{product-name}}. Как выпустить токен, читайте в разделе [Управление токенами](../../user-guide/storage/auth#token-management). Обычно токен хранится в файле `~/.yt/token`.
- MCP-совместимый AI-ассистент с подключённой LLM-моделью: Cline в Visual Studio Code, Cursor, Claude Desktop. Без подключённой модели ассистент не отвечает, поэтому модель подключается в [Шаге 2](#configure-cline).

## Шаг 1. Установка пакета {#install-package}

Установите пакет `ytsaurus-mcp`:

```bash
pip3 install ytsaurus-mcp
```

На свежих сборках Python, например на 3.14, установка системным `pip3` может завершиться ошибкой при сборке зависимостей. Если это произошло, установите пакет через `python3 -m pip`:

```bash
python3 -m pip install ytsaurus-mcp
```

Затем найдите абсолютный путь к исполняемому файлу — он понадобится в параметре `command` конфигурации MCP-клиента. Выполните команду:

{% list tabs group=defaultTabsGroup-ouitjm5w %}

- macOS / Linux

  ```bash
  which mcp_yt_server
  ```

- Windows PowerShell

  ```powershell
  (Get-Command mcp_yt_server).Source
  ```

{% endlist %}

Команда возвращает абсолютный путь к исполняемому файлу, например:

```bash
$ which mcp_yt_server

/usr/local/bin/mcp_yt_server
```

Запишите полученный путь — он пригодится в [Шаге 2](#configure-cline).

Убедитесь, что сервер установлен корректно. Для этого выведите список доступных инструментов — команда напечатает их и завершится:

```bash
$ mcp_yt_server --show-tools
```

{% cut "Пример вывода" %}

```text
Tools:
- list_dir (ListDir)
- find (Search)
- get_attributes_account (GetAttributes)
- get_attributes_account_limits_disk (GetAttributes)
- get_attributes_bundle (GetAttributes)
- get_attributes_pool (GetAttributes)
- check_is_paths_exist (CheckIsPathsExists)
- common_client_get_table_schema (CommonCypress)
- common_client_read_table (CommonCypress)
- common_client_sample_static_table (CommonCypress)
- common_client_infer_table_schema (CommonCypress)
- common_client_whoami (CommonCypress)
- check_permission (CheckPermissions)
- get_account_property (AccountProperty)
- get_proxy (GetProxy)
```

{% endcut %}

Сервер не привязывается к конкретному кластеру: имя кластера передаётся в каждом запросе к инструменту. Как задать кластер в запросе к ассистенту — в [Шаге 3](#check).

## Шаг 2. Настройка MCP-клиента на примере Cline {#configure-cline}

Настройка состоит из трёх частей. Сначала устанавливается расширение Cline, затем подключается LLM-модель и только потом настраивается сам MCP-сервер. Без LLM-модели ассистент не отвечает и не вызывает инструменты.

### 2.1. Установка расширения Cline {#install-cline}

1. Установите расширение [Cline](https://marketplace.visualstudio.com/items?itemName=saoudrizwan.claude-dev) из маркетплейса Visual Studio Code или из панели расширений VS Code.

1. Перезапустите Visual Studio Code.

### 2.2. Подключение LLM-модели {#configure-llm}

{% note warning "Без LLM-модели ассистент не работает" %}

Подключите LLM-провайдера до настройки MCP-сервера. Иначе ассистент не будет отвечать на запросы и не сможет вызывать инструменты.

{% endnote %}

Чтобы подключить LLM-модель:

1. На боковой панели Visual Studio Code откройте плагин Cline.

1. Нажмите значок шестерёнки **Settings**.

1. Укажите API-ключ провайдера и выберите модель для планирования и действий.

1. Нажмите **Done**.

### 2.3. Добавление MCP-сервера {#add-mcp-server}

Чтобы добавить MCP-сервер:

1. На боковой панели Visual Studio Code откройте плагин Cline.

1. Нажмите значок гаечного ключа — откроется вкладка **Customize**.

1. В открывшемся окне выберите **MCP** — здесь задаются настройки MCP-серверов.

1. Нажмите **Edit Configuration** — откроется файл `cline_mcp_settings.json`. Добавьте конфигурацию. Подставьте абсолютные пути, полученные в [Шаге 1](#install-package):

   - `command` — абсолютный путь к исполняемому файлу `mcp_yt_server` из вывода `which mcp_yt_server`;
   - `--yt-token-file` — абсолютный путь к файлу токена. Аргумент необязателен, если токен задан в переменной окружения `MCP_YT_TOKEN`. Подробнее читайте в разделе [Продвинутые настройки](#advanced).

   {% cut "Пример конфигурации" %}
   
   ```json
   {
     "mcpServers": {
       "local-yt-server-python": {
         "env": {},
         "args": [
           "--log-file=/tmp/out.log",
           "--log-level=DEBUG",
           "--yt-token-file=/Users/ivan/.yt/token"
         ],
         "command": "/usr/local/bin/mcp_yt_server",
         "disabled": false,
         "alwaysAllow": [],
         "type": "stdio"
       }
     }
   }
   ```

   {% endcut %}

   Меняйте только `command` и `--yt-token-file` — остальные поля оставьте как есть. Аргументы `--log-file` и `--log-level` опциональны: они включают запись отладочного лога и нужны только для диагностики проблем. О дополнительных возможностях сервера читайте в разделе [Продвинутые настройки](#advanced).

1. Сохраните файл и нажмите **Done**.

1. Убедитесь, что сервер появился в списке серверов на вкладке **Installed** и горит зелёным индикатором — это означает успешный запуск. Если индикатор красный, проверьте пути к исполняемому файлу и токену в конфигурации.

## Шаг 3. Проверка работы {#check}

Убедитесь, что после добавления в [Шаге 2](#configure-cline) сервер в списке **Installed** горит зелёным индикатором. Затем напишите ассистенту простой запрос, например:

«Покажи содержимое директории `//home` на кластере `<имя-кластера>`»

Кластер — это параметр каждого вызова инструмента, а не глобальная настройка сервера. Поэтому имя кластера указывается в тексте запроса к ассистенту, а не в конфигурации `mcpServers`.

Если ассистент вернул список объектов — сервер работает корректно. Если появилась ошибка авторизации, проверьте путь к файлу токена в конфигурации и убедитесь, что токен действует. Подробнее читайте в разделе [Управление токенами](https://ytsaurus.tech/docs/ru/user-guide/storage/auth#token-management).

Полный список доступных инструментов и примеры запросов смотрите в разделе [Методы MCP-сервера](../../user-guide/ai/mcp-server.md).

## Продвинутые настройки {#advanced}

Эти параметры не нужны для базовой работы — настраивайте их по необходимости.

{% note info "Сервер работает только на чтение" %}

MCP‑сервер {{product-name}} ограничен операциями чтения данных и конфигов и не выполняет модификацию или удаление объектов.

{% endnote %}

### Указание токена через переменную окружения {#token-env}

Вместо файла токена `--yt-token-file` токен можно передать через переменную окружения `MCP_YT_TOKEN` в поле `env` конфигурации:

```json
"env": { "MCP_YT_TOKEN": "<токен>" }
```

Источники токена сервер проверяет в следующем порядке:

1. Переменная окружения `MCP_YT_TOKEN` — наивысший приоритет.
1. Аргумент `--yt-token-file`.
1. Значение по умолчанию клиента `yt` — файл `~/.yt/token` или переменная `YT_TOKEN`.

Переменную `MCP_YT_TOKEN` читает сам MCP-сервер. Переменная `YT_TOKEN` относится к библиотеке `ytsaurus-client` и используется только как запасной вариант, когда ни `MCP_YT_TOKEN`, ни `--yt-token-file` не заданы.

### Выбор группы инструментов {#tools-groups}

По умолчанию включены все инструменты из трёх групп: `common`, `account` и `admin`. Чтобы включить только определённые группы, укажите соответствующие флаги в `args`:

#|
|| **Флаг** | **Включаемые инструменты** ||
|| `--tools-common` | Общие инструменты для работы с путями, таблицами и кластерами ||
|| `--tools-account` | Инструменты для работы с аккаунтами ||
|| `--tools-admin` | Инструменты для администраторов ||
|#

Если указан хотя бы один флаг `--tools-*`, включаются только выбранные группы. Если не указан ни один флаг — включаются все три группы.

### Транспорт {#transport}

По умолчанию сервер работает по транспорту `stdio` — этот режим используется в Cline и большинстве локальных MCP-клиентов. Для сетевого доступа используйте `sse`:

```json
"args": ["--server-transport=sse"]
```

### Логирование {#logging}

Для диагностики проблем включите запись отладочного лога:

#|
|| **Аргумент** | **Описание** ||
|| `--log-file=<путь>` | Путь к файлу лога, например `/tmp/mcp_yt_server.log` ||
|| `--log-level=<уровень>` | Уровень детализации лога: `INFO`, `ERROR` или `DEBUG` ||
|#

Пример:

```json
"args": [
  "--log-file=/tmp/mcp_yt_server.log",
  "--log-level=DEBUG"
]
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
