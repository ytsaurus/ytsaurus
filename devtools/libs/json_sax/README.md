Переработанная версия библиотеки [library/cpp/json/fast_sax](https://a.yandex-team.ru/arcadia/library/cpp/json/fast_sax).
Основная цель: сэкономить память на загрузке больших json.
Доработать существующую библиотеку сложно из-за оптимизации копирования строк (см. `enum EStoredStr` и логику вокруг него).
Попытки что-то там поменять совместимым образом превращают код в сложно читаемую лапшу.

Ключевые изменения относительно оригинала:
- добавлена возможность чтения напрямую из потока без загрузки всего json в память.
- убраны бесполезные возможности чтения нестандартных json: с комментариями, строк без кавычек, строк в одинарных кавычках.

Варианты применения:
- чтение сжатого или несжатого json с диска.
- получение json по сети или пайпу и парсинг по мере получения.

Парсинг может быть крайне неэффективен, если порции, которыми поток выдаёт данные, меньше типичного размера токена (строки или числа).

Некоторые замеры (см. подкаталог bench).
Fast - библиотечный ReadJsonFast()
Rapid - ReadJson(), под капотом которого rapidjson.
Streaming - описываемая реализация.
```
$ ls -lh ~/dev/tmp/graph.json
-rw-rw-r-- 1 say say 3,1G янв 15 09:53 /home/say/dev/tmp/graph.json

$ ya run devtools/libs/json_sax/bench ~/dev/tmp/graph.json
Fast: 9.590s: 5.993s (read) + 3.597s (parse)
Rapid: 21.710s
Streaming: 5.330s

# Этот же файл, но предварительно сжат ya tool uc

$ ls -lh ~/dev/tmp/graph.json.uc
-rw-rw-r-- 1 say say 160M фев  1 08:59 /home/say/dev/tmp/graph.json.uc

$ ya run devtools/libs/json_sax/bench ~/dev/tmp/graph.json.uc
Fast: 12.317s: 8.941s (read) + 3.375s (parse)
Rapid: 20.170s
Streaming: 5.799s
```
