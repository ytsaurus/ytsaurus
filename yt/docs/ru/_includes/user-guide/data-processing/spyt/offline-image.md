# Сборка offline Docker-образа SPYT

При обычном развёртывании SPYT дистрибутив Spark и вспомогательные артефакты скачиваются из интернета (архив Apache, Maven Central), а управляющий образ — из реестра `ghcr.io`. В закрытых средах без доступа в интернет сделать это нельзя.

Чтобы развернуть SPYT в таких условиях, используется **offline-образ** — самодостаточный Docker-образ `ghcr.io/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>`, внутрь которого уже вложены нужная сборка SPYT и дистрибутив Spark. При развёртывании такому образу доступ в интернет не нужен: все артефакты берутся из него самого.

{% note warning %}

Версия Spark должна быть не ниже `3.4.0` — только для таких версий дистрибутив вкладывается в образ. Для более старых версий Spark offline-образ не поддерживается.

{% endnote %}

## Как собрать образ {#build}

Сборка выполняется из репозитория [ytsaurus-spyt](https://github.com/ytsaurus/ytsaurus-spyt) на нужном релизном теге. Сначала склонируйте репозиторий:

```bash
git clone https://github.com/ytsaurus/ytsaurus-spyt.git
cd ytsaurus-spyt
```

В составе репозитория есть вспомогательный скрипт [`tools/release/build_offline_image.sh`](https://github.com/ytsaurus/ytsaurus-spyt/blob/main/tools/release/build_offline_image.sh), который выполняет все шаги сборки. Запускайте его из корня склонированного репозитория (либо укажите путь к нему флагом `--repo <dir>`).

Например, чтобы собрать образ для SPYT `2.9.2` и Spark `3.5.8`:

```bash
tools/release/build_offline_image.sh --spyt-version 2.9.2 --spark-version 3.5.8 --checkout
```

На выходе создаётся локальный образ `ghcr.io/ytsaurus/spyt:2.9.2-pyspark-3.5.8`.

Флаг `--checkout` **необязателен**. Если его передать, скрипт сам переключит репозиторий на релизный тег `spyt/<spyt-version>` (`git fetch --tags` + `git checkout`); при этом в отслеживаемых файлах не должно быть незакоммиченных изменений, иначе скрипт остановится с ошибкой. Если флаг не передавать, сборка пойдёт из текущего состояния рабочей копии — так удобно, когда вы уже находитесь на нужном теге или собираете образ из своей ветки. Флаги `--spyt-version` и `--spark-version` обязательны в любом случае: они задают версию артефактов SPYT, версию вкладываемого дистрибутива Spark и тег образа.

### Шаги сборки {#build-steps}

Скрипт последовательно выполняет:

1. `git checkout tags/spyt/<spyt-version>` — переключение на релизный тег (только с флагом `--checkout`).
2. `./gradlew assemble -PcustomSpytVersion=<spyt-version>` — сборка артефактов SPYT (нужен JDK 17).
3. Загрузка вспомогательного дистрибутива Livy, если он требуется Dockerfile-ом данной версии.
4. `tools/release/spyt_image/build.sh --spyt-version <spyt-version> --spark-version <spark-version>` — сборка образа с вложенным дистрибутивом Spark.
5. Проверка, что образ действительно собрался.

Эти шаги при необходимости можно выполнить и вручную, без скрипта.

## Публикация в собственный реестр {#registry}

Собранный образ доступен только локально. В изолированной среде хост развёртывания обычно не имеет доступа к `ghcr.io`, но имеет доступ к внутреннему (корпоративному) Docker-реестру. Опубликуйте offline-образ в такой реестр, чтобы разворачивать SPYT из него, а не из публичного `ghcr.io`.

Проставьте образу тег вашего реестра и запушьте:

```bash
docker tag ghcr.io/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version> \
  <your-registry>/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>
docker push <your-registry>/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>
```

Например, для SPYT `2.9.2`, Spark `3.5.8` и реестра `registry.example.com`:

```bash
docker tag ghcr.io/ytsaurus/spyt:2.9.2-pyspark-3.5.8 \
  registry.example.com/ytsaurus/spyt:2.9.2-pyspark-3.5.8
docker push registry.example.com/ytsaurus/spyt:2.9.2-pyspark-3.5.8
```

Либо соберите образ сразу под нужный реестр — передайте его префикс в `--image-cr`, а `--push` опубликует результат:

```bash
tools/release/build_offline_image.sh \
  --spyt-version <spyt-version> --spark-version <spark-version> \
  --image-cr <your-registry>/ --push --checkout
```

Например:

```bash
tools/release/build_offline_image.sh \
  --spyt-version 2.9.2 --spark-version 3.5.8 \
  --image-cr registry.example.com/ --push --checkout
```

Полученный образ `<your-registry>/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>` указывайте при развёртывании.

## Как использовать {#deploy}

Готовый образ разворачивает SPYT и дистрибутив Spark на кластер {{product-name}}. Запустите контейнер из того образа, который собрали и опубликовали, указав адрес прокси и параметры доступа:

```bash
docker run --network=host \
  -e YT_PROXY=<cluster-proxy> \
  -e YT_USER=<user> \
  -e YT_TOKEN=<token> \
  -e SPARK_DISTRIB_OFFLINE=true \
  -e SPARK_DISTRIB_VERSIONS=<spark-version> \
  --rm <your-registry>/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>
```

Последний аргумент — образ, который запускается; в изолированном контуре это образ из вашего реестра (см. [Публикация в собственный реестр](#registry)). Флаг `--rm` необязателен — он удаляет контейнер после завершения: развёртывание разовое, контейнер отрабатывает и выходит.

Если публикации в реестр не было, а сборка и развёртывание выполняются на одной машине — укажите локально собранный образ (его тег по умолчанию — `ghcr.io/ytsaurus/spyt:<spyt-version>-pyspark-<spark-version>`). `docker run` возьмёт готовый образ из локального кэша Docker на этой машине и ничего не будет скачивать.

Например, для SPYT `2.9.2`, Spark `3.5.8` и реестра `registry.example.com`:

```bash
docker run --network=host \
  -e YT_PROXY=cluster.example.com \
  -e YT_USER=spyt-deployer \
  -e YT_TOKEN=<token> \
  -e SPARK_DISTRIB_OFFLINE=true \
  -e SPARK_DISTRIB_VERSIONS=3.5.8 \
  --rm registry.example.com/ytsaurus/spyt:2.9.2-pyspark-3.5.8
```

Публикацией дистрибутива Spark на кластер управляют переменные окружения:

| Переменная | Назначение |
| --- | --- |
| `SPARK_DISTRIB_OFFLINE=true` | Полностью offline-режим: использовать только вложенный в образ дистрибутив Spark, ничего не скачивать. |
| `SPARK_DISTRIB_VERSIONS` | Список версий Spark (через пробел), которые нужно опубликовать на кластер. |

## Как проверить, что развёртывание прошло успешно {#check}

Контейнер отрабатывает и завершается с нулевым кодом возврата. В логе контейнера сообщение `Publication finished successfully` означает, что опубликованы файлы SPYT; после него контейнер загружает на кластер дистрибутив Spark. Ненулевой код возврата означает, что развёртывание не состоялось.

Дополнительно проверьте, что на кластере появились артефакты (пути указаны для SPYT `2.9.2` и Spark `3.5.8`):

```bash
yt list //home/spark/spyt/releases/2.9.2      # spyt-package.zip, setup-spyt-env.sh
yt list //home/spark/conf/releases/2.9.2      # spark-launch-conf и sidecar-конфиги
yt list //home/spark/distrib/3/5/8            # spark-3.5.8-bin-hadoop3.tgz, spark-connect_2.12-3.5.8.jar
```

После этого можно [запускать Spark-приложения](../../../../user-guide/data-processing/spyt/launch.md) обычным образом.
