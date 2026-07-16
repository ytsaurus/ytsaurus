# Word Count в {{product-name}} Flow (C++)

Простейший вводный пример на C++. [Пайплайн]({{source-root}}/yt/yt/flow/examples/cpp/word_count) читает текстовые сообщения из входной очереди, разбивает их на слова и подсчитывает количество вхождений каждого слова с помощью внешней таблицы стейта.

[Исходный код]({{source-root}}/yt/yt/flow/examples/cpp/word_count)

Обе единицы пользовательской логики написаны как [process function](../../../../flow/cpp/process-functions.md) (наследники `IProcessFunction`): сама функция лёгкая и тестируемая, а режим её исполнения выбирается встроенным `Computation`-адаптером в спеке через поле `processing_function`.

## Компоненты пайплайна

### TTextReadFunction

`TTextReadFunction` &mdash; process function (`IProcessFunction`), которую исполняет `TProcessFunctionSourceComputation` (source-адаптер). Она читает текстовые сообщения из входной очереди, разбивает текст на слова (по пробельным символам) и для каждого слова длиной не меньше `min_word_length` генерирует объект `TWordMessage` в выходной поток `words`. Параметр `min_word_length` читается в `Init` через `initContext->GetParameters<TTextReaderParameters>()` из блока `processing_function_parameters` спеки.

Так как исполняющий `Computation` является источником (`TSwiftOrderedSourceComputation`), выходные сообщения не сохраняются в {{product-name}} &mdash; сохраняется только метаинформация, необходимая для детерминированной работы. Подробнее про типы компьютейшенов можно прочитать в разделе [Компьютейшены](../../../../flow/concepts/computation.md).

### TWordCountFunction

`TWordCountFunction` &mdash; process function (`IProcessFunction`), которую исполняет `TProcessFunctionComputation` (transform-адаптер). Она использует `TSimpleExternalStateManager` для работы со [стейтом](../../../../flow/concepts/glossary.md#state). Для каждого входного слова она:
1. Получает текущее значение счетчика из внешней таблицы стейта
2. Увеличивает счетчик на 1
3. Записывает обновленное значение обратно

Подробнее про работу со стейтом &mdash; в разделе [Стейт](../../../../flow/concepts/stateful.md).

## Типы сообщений

`TWordMessage` &mdash; наследник `TYsonMessage`. Содержит единственное поле `Word`. Регистрируется через макрос `YT_FLOW_DEFINE_YSON_MESSAGE`.

## Структура пайплайна

Пайплайн состоит из двух компьютейшенов-адаптеров, соединенных потоком `words`:

1. **source** (входная очередь) &rarr; **TProcessFunctionSourceComputation** (`processing_function = TTextReadFunction`) &rarr; поток `words`
2. Поток `words` &rarr; **TProcessFunctionComputation** (`processing_function = TWordCountFunction`) &rarr; таблица стейта (word &rarr; count)

В спеке для счётчика указывается `group_by_schema` с хэшем от слова и самим словом &mdash; для корректного [партиционирования](../../../../flow/concepts/glossary.md#partition). Параметры стейта (`TSimpleExternalStateManager` и путь к таблице) задаются декларативно в секции `external_state_managers` спеки `Computation`.

## Функция main

В `main` выполняется:
1. `NYT::NFlow::Initialize(argc, argv)` &mdash; инициализация библиотеки Flow
2. Регистрация функций через `YT_FLOW_DEFINE_PROCESS_FUNCTION(TTextReadFunction)` и `YT_FLOW_DEFINE_PROCESS_FUNCTION(TWordCountFunction)`
3. `TSimpleSpecBuilder` &mdash; билдер для регистрации потоков. Через `RegisterStream<TWordMessage>("words")` регистрируется поток `words` с типом сообщений `TWordMessage`
4. `TSimpleRunnerProgram` &mdash; запуск пайплайна

## Исходный код

### TTextReadFunction

{% code '/yt/yt/flow/examples/cpp/word_count/lib/word_count_functions.cpp' lang='cpp' lines='[BEGIN text_reader]-[END text_reader]' keep-indents %}

### TWordCountFunction

{% code '/yt/yt/flow/examples/cpp/word_count/lib/word_count_functions.cpp' lang='cpp' lines='[BEGIN word_counter]-[END word_counter]' keep-indents %}

## См. также

- [Быстрый старт (C++)](../../../../flow/cpp/getting-started.md)
- [Computation (C++)](../../../../flow/cpp/computation.md)
- [Stateful processing](../../../../flow/concepts/stateful.md)
