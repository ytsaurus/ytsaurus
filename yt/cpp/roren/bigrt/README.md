# Как начать
Первым делом нужно продумать откуда вы хотите читать и куда писать.
Далее это понимание нужно будет описать через конфиг [сапплайера](https://a.yandex-team.ru/arcadia/bigrt/lib/supplier/config/supplier_config.proto?rev=r13829762#L96) ([пример](https://a.yandex-team.ru/arcadia/bigrt/deploy/testing/swift_load/swift_load_config_template.yaml?rev=r13797268#L55)) и [писателя](https://a.yandex-team.ru/arcadia/yt/cpp/roren/bigrt/proto/config.proto?rev=r13656196#L39) ([пример](https://a.yandex-team.ru/arcadia/bigrt/deploy/testing/swift_load/swift_load_config_template.yaml?rev=r13797268#L85)).
В примере писатели/читатели задаются через соответствующие поля [рорен конфига](https://a.yandex-team.ru/arcadia/yt/cpp/roren/bigrt/proto/config.proto), в [коде](https://a.yandex-team.ru/arcadia/bigrt/demo/swift_load/main.cpp?rev=r13797268#L205) к писателям/читателям следует обращаться через заданные им имена.

После этого нужно в коде описать пайлайн того как данные должны быть преобразованы. Преобразования описываются кубиками, которые композируются используя пайп(|). Результатом каждого применения пайпа является поток данных, который посредством наложения на себя следующего кубика преобразуется в новый поток. В рорене есть ряд готовых кубиков, пользователь может создавать собственные кубики из [лямбд](https://a.yandex-team.ru/arcadia/bigrt/demo/swift_load/main.cpp?rev=r13797268#L206) или создавая специальные [классы](https://a.yandex-team.ru/arcadia/bigrt/demo/swift_load/main.cpp?rev=r13797268#L158).

# Примеры
- [Фильтр пишущий в qyt/lb](https://a.yandex-team.ru/arcadia/yt/cpp/roren/bigrt/test_medium/filter_pipeline/filter_pipeline.cpp). Читаем из дефолтного входа input, разбиваем входные батчи на множество строчек, далее фильтруем строчки и пишем их используя кубики для записи в qyt/lb.

- [Джойн с таблицей в YT.](https://a.yandex-team.ru/arcadia/yt/cpp/roren/bigrt/test_medium/in_memory_dict_resolver/in_memory_dict_resolver.cpp)
Входные строчки джойним с таблицей на ыте используя пару BindToProtoDynamicTableInMemory и DictJoin.
- [Сбор метрик.](https://a.yandex-team.ru/arcadia/yt/cpp/roren/bigrt/test_medium/metrics_pipeline/metrics_pipeline.cpp)
Пример того как можно рисовать графики в monitoring используя ытевую библиотеку профилирования.
- [Чтение с нескольких входов в рамках одного процессинга.](https://a.yandex-team.ru/arcadia/yt/cpp/roren/bigrt/test_medium/multiple_consuming_systems/multiple_consuming_systems.cpp)
Чтобы читать несколько входов одновременно, нужно описать их все в мапе EasyInputs, затем для каждого имени входа позвать ReadMessageBatch и описать пайплайн обработки.
- [Простой стейтфул.](https://a.yandex-team.ru/arcadia/yt/cpp/roren/bigrt/test_medium/stateful_pipeline/stateful_pipeline.cpp)
Пример как сохранять простой стейт на ыте.
- [Запись в таблицу в YT](https://a.yandex-team.ru/arcadia/yt/cpp/roren/bigrt/test_medium/write_ytdyntable_node/main.cpp)
Пример как в качестве выхода использовать таблицу в YT.
- [Демо решадрера.](https://a.yandex-team.ru/arcadia/bigrt/demo/resharder)
Шаблон для процессинга без стейта, если вам нужен маппер, стоит отталкиваться от этого. Пишет в swift/qyt используя встроенные в рорен конфиг описания писателей.
- [Демо записи/чтения Swift + stateful.](https://a.yandex-team.ru/arcadia/bigrt/demo/swift_load)
Пример statetuf/stateless процессинга, описанных в одном бинаре и коммуницирующих через swift очередь.
- [BigB Resharder.](https://a.yandex-team.ru/arcadia/ads/bsyeti/resharder/roren-bin)
Большой stateless процессинг рекламы.
