# Word Count в {{product-name}} Flow (Go)

Простейший пример stateful-[пайплайна](../../../../flow/concepts/glossary.md#pipeline) на Go: подсчёт количества вхождений каждого слова во внутреннем YSON-[стейте](../../../../flow/concepts/glossary.md#state).

[Исходный код]({{source-root}}/yt/yt/flow/examples/go/word_count)

## Структура {#structure}

Пайплайн состоит из двух [компьютейшенов](../../../../flow/concepts/glossary.md#stream-and-computation):

- `reader` — нативный сорс (`TSwiftPassthroughOrderedSourceComputation`), объявленный прямо в [спеке](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec): он читает очередь и публикует строки в [стрим](../../../../flow/concepts/glossary.md#stream-and-computation) `words`. Go-кода у него нет.
- `mapper` — transform-компьютейшен (`TTransformCompanionComputation`), который обслуживает компаньон: он читает стрим `words` и обновляет счётчик слова во внутреннем стейте.

Сообщения группируются по слову (`group_by_schema` с `farm_hash(word)` и `word`), поэтому стейт обрабатываемого ключа — это счётчик ровно одного слова. Результат пайплайна лежит в таблице внутреннего стейта: дальше по графу ничего не отправляется.

## `main.go` {#main-go}

Точка входа: создание пайплайна, регистрация единственного компьютейшена и запуск.

{% code '/yt/yt/flow/examples/go/word_count/main.go' lang='go' %}

## `word_count_mapper.go` {#word-count-mapper-go}

Значение стейта — обычная Go-структура с YSON-тегами: именно в этом виде она лежит в таблице внутреннего стейта.

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_state]-[END word_count_state]' %}

`flow.RowFunction`, которая открывает внутренний стейт по имени `word-state` через `flow.OpenYSONState` и увеличивает счётчик:

{% code '/yt/yt/flow/examples/go/word_count/word_count_mapper.go' lang='go' lines='[BEGIN word_count_mapper]-[END word_count_mapper]' %}

## Ключевые паттерны {#key-patterns}

- Простейший stateful-пайплайн с одним компьютейшеном на стороне компаньона: сорс остаётся нативным, и Go-кода для него не требуется.
- Внутренний YSON-стейт через `flow.OpenYSONState[T](rt, name, msg)`: `Value()` возвращает изменяемую структуру, которую SDK сохраняет после успешного батча.
- Имя стейта (`word-state`) совпадает с именем из `parameters.internal_states` компьютейшена в спеке.
- Ключ стейта определяется `group_by_schema` из [спеки](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec) — в данном случае по полю `word`.
- Вход один раз преобразуется в `wordMessage` через `msg.ConvertTo(&input)`, после чего обработчик работает с полями структуры.
