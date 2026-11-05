## Параметры запуска {#test-param}

Поведение интеграционных тестов настраивается через `--test-param NAME=VAL`:

#|
|| **Параметр** | **По умолчанию** | **Значения** | **Действие** ||
|| `RUNNER_LOG_LEVEL` | | `Error`, `Info`, `Debug`, … | Уровень логирования процесса, запускающего пайплайн (runner). ||
|| `PAUSE_BEFORE_FLOW_PROCESS_FEDERATION_TEARDOWN` | `0` | `0`, `1` | Зависнуть в тесте перед остановкой процессов Flow. В комбинации с `--test-disable-timeout` позволяет надолго оставить работающий локальный {{product-name}} и федерацию процессов Flow для неспешного изучения через UI. ||
|| `EXTERNAL_YT_CONFIG` | (не задан) | yson — см. ниже | Запускать пайплайн на реальных внешних кластерах {{product-name}} вместо локального рецепта. ||
|#

Примеры:

```bash
ya make -A --test-param RUNNER_LOG_LEVEL=Debug
ya make -A --test-disable-timeout --test-param PAUSE_BEFORE_FLOW_PROCESS_FEDERATION_TEARDOWN=1
```

### `EXTERNAL_YT_CONFIG` {#external-yt-config}

Локальный {{product-name}} всё ещё стартует рецептом, но тест его игнорирует.

Обязательные поля, общие для всех кластеров:

- `path` — базовая директория.
- `tablet_cell_bundle` — bundle создаваемых динамических таблиц.
- `proxy_role` — RPC proxy role.

Опционально: `primary_medium` (по умолчанию `"default"`).

Список `clusters` — первый элемент primary. Поля записи: `cluster_name` (обязательно), `proxy_url` (по умолчанию равен `cluster_name`).

Авторизация: при внешнем {{product-name}} `YT_TOKEN`/`YT_USER` из локального рецепта чистятся, yt-wrapper подхватывает токен из `~/.yt/token`.

Изоляция: `work_yt_path = path/<local-username>/<test_name>`; директория `path/<username>` удаляется и пересоздаётся один раз на класс в `setup_class`.

Пример:

```bash
ya make -A --test-param 'EXTERNAL_YT_CONFIG={path="//tmp/yt_flow";tablet_cell_bundle=default;proxy_role=default;clusters=[{cluster_name={{production-cluster}}};];}'
```
