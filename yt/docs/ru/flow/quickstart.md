# Быстрый старт с {{product-name}} Flow

Запустите пример C++ Word Count как первый пайплайн. Он читает строки текста из очереди, разделяет их на слова и сохраняет счётчики по словам в динамической таблице. Понадобятся доступ к кластеру {{product-name}} и пул, в котором можно запустить [Vanilla-операцию](devops/vanilla/initial-deploy.md#prerequisites).

## Соберите пример {#build}

Из [исходного репозитория]({{source-root}}) соберите бинарь примера:

```bash
ya make yt/yt/flow/examples/cpp/word_count
```

Бинарь появится по пути `yt/yt/flow/examples/cpp/word_count/word_count`. В параметрах `processing_function_parameters` компьютейшена `reader` задано `min_word_length = 4`, поэтому слова короче четырёх байт не учитываются; чтобы считать все слова, установите значение `0`. [Руководство по C++](cpp/getting-started.md#define-messages) объясняет код, компьютейшены и стейт.

## Подготовьте пайплайн {#prepare}

Выберите существующий каталог Cypress, в который у вас есть права на запись. В командах ниже `//home/flow/word-count` — пример базового пути: замените его везде на свой каталог, а `<cluster>` — на имя прокси своего кластера. По [схеме из примера Word Count]({{source-root}}/yt/yt/flow/examples/cpp/word_count/test/yt_sync.py) создайте входную очередь, консьюмера и сортированную внешнюю таблицу стейта:

```bash
yt --proxy <cluster> create table //home/flow/word-count/input_queue --attributes '{dynamic=%true;schema=[{name=text;type=string};{name="$timestamp";type=uint64};{name="$cumulative_data_weight";type=int64}]}'
yt --proxy <cluster> create queue_consumer //home/flow/word-count/consumer
yt --proxy <cluster> register-queue-consumer //home/flow/word-count/input_queue //home/flow/word-count/consumer --vital
yt --proxy <cluster> create table //home/flow/word-count/word_counts --attributes '{dynamic=%true;schema=[{name=hash;type=uint64;sort_order=ascending;expression="farm_hash(word)"};{name=word;type=string;sort_order=ascending};{name=count;type=int64}]}'
yt --proxy <cluster> mount-table //home/flow/word-count/input_queue
yt --proxy <cluster> mount-table //home/flow/word-count/word_counts
```

Ключевые колонки `hash` и `word` совпадают с `group_by_schema` компьютейшена `counter`; `count` хранит значение. В очереди есть входная колонка `text` и две системные колонки, описанные в [Queue API](../user-guide/dynamic-tables/queues.md#api). Команда `create queue_consumer` сразу монтирует таблицу консьюмера. Флаг `--vital` запрещает автоматическое удаление строк очереди, которые этот консьюмер ещё не прочитал. Перед следующим шагом проверьте существование всех трёх объектов:

```bash
yt --proxy <cluster> exists //home/flow/word-count/input_queue
yt --proxy <cluster> exists //home/flow/word-count/consumer
yt --proxy <cluster> exists //home/flow/word-count/word_counts
```

Каждая команда должна вывести `true`. Этот C++-раннер при первом запуске создаёт объект пайплайна и внутренние таблицы, а затем монтирует их. Если вы хотите создать объект заранее, см. [создание пайплайна](concepts/pipeline-object.md#create).

Откройте `yt/yt/flow/examples/cpp/word_count/pipeline.yson`. Добавьте `cluster_url` и путь `path` к пайплайну на своём кластере. Укажите для `queue_path` и `consumer_path` значения `<cluster=cluster_name>//home/flow/word-count/input_queue` и `<cluster=cluster_name>//home/flow/word-count/consumer`, заменив `cluster_name` настоящим именем кластера. Замените путь внешнего стейта `//path/to/word_counts` на созданную выше таблицу. Во всех путях используйте один и тот же выбранный вами каталог.

Добавьте в конфиг секцию `vanilla` верхнего уровня с `enable = %true`, именем своего пула и числом воркеров. Пример секции и параметры ресурсов приведены в [руководстве по первичному деплою](devops/vanilla/initial-deploy.md#enable).

```yson
"cluster_url" = "<your-cluster>";
"path" = "//home/flow/word-count/pipeline";
"vanilla" = {
    "enable" = %true;
    "pool" = "<your-pool>";
    "worker" = {"count" = 1};
};
```

Поместите эти поля внутрь существующих внешних фигурных скобок `pipeline.yson` и замените шаблоны своим кластером и пулом. Если вы выбрали другой базовый каталог, замените и пример пути к пайплайну.

Если джобы кластера работают в Docker/CRI под Kubernetes, до запуска проверьте [разрешение имени кластера](devops/vanilla/docker-environment.md#cluster-name) внутри джоб и [доступ к прокси извне](devops/vanilla/docker-environment.md#external-access) для раннера. Бинарю C++ не нужен отдельный Docker-образ; добавляйте сетевые настройки из руководства, только если они требуются вашей установке.

## Проверьте и запустите {#launch}

Для этого C++-раннера `--validate-only` проверяет конфиг локально, не запуская и не обновляя пайплайн:

```bash
./yt/yt/flow/examples/cpp/word_count/word_count --config yt/yt/flow/examples/cpp/word_count/pipeline.yson --validate-only
```

После проверки запустите пайплайн:

```bash
YT_FLOW_WAIT=0 ./yt/yt/flow/examples/cpp/word_count/word_count --config yt/yt/flow/examples/cpp/word_count/pipeline.yson
```

С `YT_FLOW_WAIT=0` раннер завершится после запуска. Без этой переменной действует значение по умолчанию `YT_FLOW_WAIT=1`: раннер остаётся в терминале и выводит новые записи лога контроллера, пока пайплайн работает. Прерывание раннера не останавливает Vanilla-операцию. Проверьте создание и состояние пайплайна:

```bash
yt --proxy <cluster> exists //home/flow/word-count/pipeline
yt --proxy <cluster> flow get-pipeline-state //home/flow/word-count/pipeline
```

Первая команда должна вывести `true`, вторая — `working`. Отправьте строку в очередь и после следующей эпохи обработки проверьте счётчики:

```bash
echo '{text="hello world hello"}' | yt --proxy <cluster> insert-rows //home/flow/word-count/input_queue --format yson
yt --proxy <cluster> select-rows '* from [//home/flow/word-count/word_counts]' --format json
```

Ожидаются `hello` со значением `count` 2 и `world` со значением `count` 1. Если таблица пока пуста, дождитесь следующей эпохи обработки и повторите запрос; при ошибках джоб или отсутствии прогресса перейдите к [диагностике](devops/diagnostics.md). Работающие джобы можно также проверить в UI {{product-name}}. После знакомства с примером [удалите узел Flow](devops/vanilla/pipeline-operations.md#remove) и отдельно решите, нужны ли вам созданные очередь, консьюмер и таблица внешнего стейта.

Для другого языка перейдите к его руководству: [Go](go/getting-started.md), [Java](java/getting-started.md), [Python](python/getting-started.md) или [YQL](yql/getting-started.md).
