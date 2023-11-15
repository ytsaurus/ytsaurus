{% include [Установка](../../_includes/api/cli/install-p1.md) %}

## Установка из PyPI-репозитория

Пакет называется `ytsaurus-client`. Перед установкой пакета можно поставить пакет `wheel`, чтобы иметь возможность поставить версию, отличную от системной, или {{product-name}} CLI без sudo.

По умолчанию из PyPI устанавливается последняя стабильная версия пакета.
Все тестовые версии имеют суффикс `a1` и могут быть установлены через pip путем добавления опции `--pre`.
Команда для установки из pypi:
  ```bash
  # Установка {{product-name}} CLI
  $ pip install ytsaurus-client
  # Установка YSON-bindings
  $ pip install ytsaurus-yson
  ```

{% include [Автодополнение](../../_includes/api/cli/install-p2.md) %}