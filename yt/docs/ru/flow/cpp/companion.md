# Запуск C++ process-функций в companion

C++ companion SDK выполняет [process-функции](process-functions.md) в отдельном процессе под управлением воркера Flow. Этот вариант подходит, когда пользовательскому коду нужен отдельный исполняемый файл и адресное пространство. Соберите companion отдельно от воркера и раннера; воркер передаёт ему батчи через [протокол companion](../concepts/companion.md).

Этот справочник предполагает, что у вас уже есть C++ process-функция и вы знакомы с настройкой и развёртыванием пайплайна. Регистрация и подключение показаны в поддерживаемом [тестовом companion с ресурсами]({{source-root}}/yt/yt/flow/tests/companion/resource/cpp/companion/main.cpp), его [сборочном файле]({{source-root}}/yt/yt/flow/tests/companion/resource/cpp/companion/ya.make) и [спецификации пайплайна]({{source-root}}/yt/yt/flow/tests/companion/resource/cpp/pipeline.yson).

## Регистрация функций и ресурсов {#registration}

Используйте `NYT::NFlow::NCompanionServer::TPipeline` из `yt/yt/flow/library/cpp/companion/server/pipeline.h`. Заполните его до запуска сервера:

- `AddTransform<TFunction, TStaticParameters, TDynamicParameters>(computationId)` объявляет обслуживаемый компьютейшен преобразования и регистрирует его process-функцию и схемы параметров.
- `AddSource<TFunction, TStaticParameters, TDynamicParameters>(computationId)` объявляет обслуживаемый компьютейшен источника. Оба типа параметров по умолчанию — `TEmptyProcessFunctionParameters`.
- `AddResource<TResource>()` регистрирует класс ресурса, размещённого в companion. Идентификаторы ресурсов задаются в спецификации, а не в этом вызове.

Идентификатор компьютейшена должен совпадать со спецификацией. Поле `processing_function` по-прежнему выбирает зарегистрированную функцию по имени C++-типа. Для этой регистрации типизированный API заменяет `YT_FLOW_DEFINE_PROCESS_FUNCTION`, а `AddResource` — `YT_FLOW_DEFINE_RESOURCE`. Не регистрируйте один тип обоими способами. Повторяющиеся идентификаторы компьютейшенов отклоняются.

Передайте заполненный пайплайн в `NYT::NFlow::NCompanionServer::RunCompanionMain(argc, argv, std::move(pipeline))` из `companion_main.h`. В `ya.make` исполняемого файла нужна зависимость от `yt/yt/flow/library/cpp/companion/server`; добавьте зависимости своих функций и ресурсов. В сборочном файле тестового companion выше приведён полный список зависимостей для этого примера.

## Подключение исполняемого файла к воркеру {#wiring}

1. Разместите исполняемый файл companion и его зависимости в окружении джоба-воркера. Используйте существующую [инструкцию развёртывания](../devops/vanilla/initial-deploy.md); путь в спецификации сам по себе не загружает исполняемый файл.
1. Объявите ресурс `NYT::NFlow::NCompanion::TCompanionManager`. В его `parameters.entrypoint` задайте `executable` — абсолютный путь к исполняемому файлу в этом окружении. Поле `args` задаёт аргументы командной строки, а `env` — дополнительные переменные окружения.
1. Для каждого компьютейшена выберите [класс companion-компьютейшена](../concepts/companion.md#vidy-computation-ov-dlya-raboty-s-kompanonami) в `computation_class_name`, задайте `processing_function` и добавьте зависимость от менеджера на воркере под псевдонимом `CompanionManager` (`worker = %true`, `controller = %false`). Идентификатор компьютейшена должен совпадать с переданным в `AddTransform` или `AddSource`.
1. Если стандартный раннер `flow_server` использует типы функций, зарегистрированные только в companion, задайте `abort_on_specs_parseability_error = %false` в конфигурации раннера. Раннер не может разобрать тип функции, которого нет в его реестре. Исполняемый файл companion всё равно должен корректно зарегистрировать выбранную функцию и схемы параметров.

В [тестовом окружении C++ companion]({{source-root}}/yt/yt/flow/library/python/integration_test_base/yt_flow_cpp_base.py) показано это разделение: раннером служит `flow_server`, а точка входа менеджера указывает на C++-файл. Интеграционный тест проверяет выходные данные со значениями, загруженными в обслуживающем процессе companion.

## Запуск и границы выполнения {#execution}

`RunCompanionMain` запускает сервер companion. Ему нужны переданные воркером `YT_FLOW_MODE=Worker` и `YT_FLOW_COMPANION_CONFIG` с положительным RPC-портом `port`. Пайплайн запускайте раннером; точка входа companion не реализует команду запуска пайплайна раннера.

C++ companion работает в одном многопоточном процессе. Обеспечьте безопасность конкурентного доступа к общим объектам.

Поддерживаемые интерфейсы и их границы описаны в разделе [Ограничения C++ companion](../concepts/companion.md#cpp-companion). Выбирайте режим размещения явно: не все возможности process-функций в воркере доступны в отдельном процессе.

## Ресурсы companion и HTTP-клиенты {#resources-and-http}

Зарегистрируйте классы ресурсов через `AddResource<TResource>()` и подключите прокси на воркере по [общей схеме](../concepts/companion.md#companion-resources). В методе `Init` process-функции вызов `IRuntimeInitContext::GetStaticResource(alias)` возвращает привязанный ресурс на стороне companion. Этот путь показан в [тестовой функции]({{source-root}}/yt/yt/flow/tests/companion/resource/cpp/companion/main.cpp). Ресурс, существующий только в воркере, не переносится в companion автоматически.

`IRuntimeInitContext::GetHttpClient()` и `GetHttpsClient()` предоставляют общие HTTP-клиенты. Задайте `http_client_config`, `https_client_config` и `http_poller_threads` в блоке `companion` конфигурации воркера; см. [TCompanionConfig](../generated_docs/all_yson_structs.md#NYT_NFlow_NCompanion_TCompanionConfig).

## Исходный код и тесты {#source-and-tests}

- [SDK сервера companion]({{source-root}}/yt/yt/flow/library/cpp/companion/server).
- [Тесты регистрации]({{source-root}}/yt/yt/flow/library/cpp/companion/server/unittest/pipeline_ut.cpp) и [тесты конфигурации запуска]({{source-root}}/yt/yt/flow/library/cpp/companion/server/unittest/config_ut.cpp).
- [Интеграционный тест ресурсов]({{source-root}}/yt/yt/flow/tests/companion/resource/cpp/test_resource.py) и [общий сценарий жизненного цикла]({{source-root}}/yt/yt/flow/tests/companion/resource/common/companion_resource_test_base.py): значения зависимостей, динамические обновления и восстановление после перезапуска companion.
