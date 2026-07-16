# Что такое {{product-name}} Flow?

{{product-name}} Flow &mdash; это фреймворк для потоковой кросс-ДЦ обработки событий с гарантиями exactly-once в рамках экосистемы {{product-name}} с API для [C++](../../flow/cpp/getting-started.md), [Java и Kotlin](../../flow/java/getting-started.md), [Python](../../flow/python/getting-started.md), а также поддержкой декларативного описания пайплайнов на [YQL](../../flow/yql/getting-started.md).{% if audience == "internal" %} Система является логическим развитием фреймворка [BigRT](https://docs.yandex-team.ru/big_rt/).{% endif %}

Ближайшими внешними аналогами являются [Google Cloud Dataflow](https://cloud.google.com/products/dataflow?skip_cache=true%22%22) и [Apache Flink](https://flink.apache.org/).{% if audience == "internal" %} В отдельной статье можно ознакомиться с [подробным сравнением с альтернативными технологиями](../../yandex-specific/flow/other/comparison.md).{% endif %}

Система находится в активной разработке, однако на её основе построили свои продакшн процессы более десяти команд{% if audience == "internal" %} из разных частей Яндекса, включая Рекламу, Маркет, Алису, ОПК, Видео Поиск{% endif %}. Система точно умеет:

- Справляться с нагрузкой свыше 100&nbsp;ГБ/с или 1 млн событий в секунду{% if audience == "internal" %} ([colibri](../../yandex-specific/flow/other/framework_users.md#colibri)){% endif %}.
- Поддерживать 150+ логических узлов пайплайна{% if audience == "internal" %} ([limbert](../../yandex-specific/flow/other/framework_users.md#limbert)){% endif %}.

## Контакты {#contact}

{% if audience == "internal" %}
По всем вопросам обращайтесь в чат в Yandex Messenger [YT Flow Public](https://nda.ya.ru/t/MBW0Jgy-7bH78f)

В случае вопроса со сложным контекстом или найденного бага создавайте тикет в очереди [YTFLOWSUPPORT](https://nda.ya.ru/t/X7imi95a7gKE5Y).
{% endif %}

## Свойства системы {#properties}

<!-- Поддерживаемый порядок свойств: продуктово-значимые, технические гарантии, инфраструктурные. -->

- Нативная поддержка многоэтапных [пайплайнов](../../flow/concepts/glossary.md#pipeline). Как следствие &mdash; более простой деплой и управление системой.
- Поддержка [вотермарков](../../flow/concepts/watermarks.md) и [таймеров](../../flow/concepts/timers.md).
- [Exactly-once семантика](../../flow/concepts/guarantees.md) обработки событий по умолчанию.
- Характерная задержка обработки событий при стабильной работе: 1с &mdash; 10с.
- Автоматическая балансировка [партиций](../../flow/concepts/glossary.md#partition) по машинам.
- Отказоустойчивость: пайплайн переживает выпадение отдельных машин и датацентров.
- Возможность реализации бизнес-логики на [C++](../../flow/cpp/getting-started.md), [Java и Kotlin](../../flow/java/getting-started.md), [Python](../../flow/python/getting-started.md) и [YQL](../../flow/yql/getting-started.md).
- Поддержка [stateful-обработки](../../flow/concepts/stateful.md) с персистентным состоянием в динамических таблицах {{product-name}}.
- Поддержка запуска и в {{product-name}}{% if audience == "internal"%}, и в [Deploy](https://docs.yandex-team.ru/deploy){% endif %}.

{% include [Выбор языка](language-choice.md) %}

## Целевые свойства системы {#target-properties}

- Умное планирование всего пайплайна целиком, с учётом CPU/RAM потребления как отдельных узлов пайплайнов, так и с учётом существования разделяемых между несколькими узлами ресурсов (общие кеши, базы и т. п.).
- Возможность запуска пайплайнов на кластерах в тысячи нод и более.
- Минимальные даунтаймы при выпадении нод, ДЦ, кластеров, а также при обновлениях.

## См. также {#see-also}

- [С чего начать](../../flow/start.md)
- [Быстрый старт](../../flow/quickstart.md)
- [Основные понятия](../../flow/concepts/glossary.md)
- [Примеры задач](../../flow/tasks.md)
{% if audience == "internal" %}- [Кто использует {{product-name}} Flow](../../yandex-specific/flow/other/framework_users.md){% endif %}
