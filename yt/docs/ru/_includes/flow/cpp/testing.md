# Тестирование в {{product-name}} Flow (C++)

{% note info %}

C++ [воркеры](../../../flow/concepts/glossary.md#worker) на данный момент не имеют отдельного фреймворка для юнит-тестирования компьютейшенов: основной подход — запускать пайплайн целиком с конечными источниками данных. Пример теста можно найти в [examples/cpp/wait_click_join]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join). Если писать юнит тесты всё же необходимо, то можно выносить бизнес-логику из компьютейшнов в отдельные классы и писать юнит тесты уже на них.

{% endnote %}

{% include notitle [_](../testing-integration-body.md) %}

{% include notitle [_](../testing-test-param-body.md) %}

## См. также

- [Базовые правила выкатки](../../../flow/release/basic-rules.md)
- [Тестирование (Java)](../../../flow/java/testing.md)
- [Тестирование (Python)](../../../flow/python/testing.md)
- [Быстрый старт (C++)](../../../flow/cpp/getting-started.md)
- [Быстрый старт (Java)](../../../flow/java/getting-started.md)
- [Быстрый старт (Python)](../../../flow/python/getting-started.md)
