# С чего начать в {{product-name}} Flow

В этом разделе пошагово описано, как во Flow реализовать и запустить свой собственный пайплайн.

{% include [Выбор языка](language-choice.md) %}

## Общий план

Независимо от выбранного языка, создание пайплайна включает следующие шаги:

1. **Попробуйте [Быстрый старт](../../flow/quickstart.md)** — запустите минимальный NoOp-пайплайн, чтобы познакомиться с инфраструктурой Flow.

2. **Ознакомьтесь с основными понятиями**. Прочитайте [глоссарий](../../flow/concepts/glossary.md), чтобы понять модель Flow: пайплайны, потоки, компьютейшены, сообщения.

3. **Изучите концепции**. Разберитесь с [Computation](../../flow/concepts/computation.md), [Watermarks и Timers](../../flow/concepts/watermarks.md) и [Stateful-обработкой](../../flow/concepts/stateful.md), а также предоставляемыми системой [гарантиями](../../flow/concepts/guarantees.md).

4. **Изучите примеры** на выбранном языке:
   - C++: [WordCount](../../flow/cpp/examples/word_count.md), [Shuffle](../../flow/cpp/examples/shuffle.md), [WaitClickJoin](../../flow/cpp/examples/wait_click_join.md)
   - Java: [WordCount](../../flow/java/examples/wordcount.md), [Shuffle](../../flow/java/examples/shuffle.md), [WaitClickJoin](../../flow/java/examples/wait_click_join.md)
   - Python: [WordCount](../../flow/python/examples/wordcount.md), [Shuffle](../../flow/python/examples/shuffle.md), [WaitClickJoin](../../flow/python/examples/wait_click_join.md)
   - YQL: [Быстрый старт](../../flow/yql/getting-started.md)

5. **Ознакомьтесь с доступными [коннекторами](../../flow/connectors/about.md)** — очереди, статические таблицы{% if audience == "internal" %}, Logbroker{% endif %} и др.

6. **Опишите спеку пайплайна** в формате YSON. Помимо примеров, вам поможет раздел [Spec & DynamicSpec](../../flow/concepts/spec.md).

7. **Реализуйте бизнес-логику** на выбранном языке, следуя соответствующему руководству по быстрому старту.

8. **Создайте необходимые объекты в {{product-name}}** — таблицы, очереди, пайплайн{% if audience == "internal" %} — с помощью утилиты [YtSync]({{yt-sync-docs}}/) (спецификация пайплайна описана [здесь]({{yt-sync-docs}}/pipeline_specification)){% endif %}.{% if audience == "internal" %} При необходимости сделайте то же самое в сторонних системах вроде [Logbroker](../../yandex-specific/flow/extensions/logbroker.md).{% endif %}

9. **Напишите тесты.** Следуйте инструкциям для выбранного языка программирования:
   - [C++](../../flow/cpp/testing.md)
   - [Java](../../flow/java/testing.md)
   - [Python](../../flow/python/testing.md)

10. **Запустите пайплайн** и следите за ним через UI {{product-name}}. Детально про релизы можно прочитать в [Релизы и управление пайплайном](../../flow/release/basic-rules.md).

## См. также

- [О Flow](../../flow/about.md)
- [Быстрый старт](../../flow/quickstart.md)
- [Основные понятия](../../flow/concepts/glossary.md)
- [Коннекторы](../../flow/connectors/about.md)
{% if audience == "internal" %}- [Сравнение с альтернативными технологиями](../../yandex-specific/flow/other/comparison.md){% endif %}