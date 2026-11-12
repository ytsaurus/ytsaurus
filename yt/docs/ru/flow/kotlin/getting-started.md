# Быстрый старт в {{product-name}} Flow (Kotlin)

Kotlin-код во Flow выполняется тем же [Java SDK](../../flow/java/getting-started.md), что и Java-код: отдельного Kotlin SDK нет. Компьютейшены, стейты, типизированные стримы, Spring Boot-интеграция и тестовые утилиты `flow-test-utils` описаны в разделе Java. На этой странице собраны Kotlin-специфичные детали: сборка, точка входа, JVM-артефакты и nullability.

[Исходный код Java SDK для Flow]({{source-root}}/yt/java/flow)

[Примеры на Kotlin]({{source-root}}/yt/yt/flow/examples/kotlin)

Для общих объектов процесса используйте [Java API ресурсов companion](../../flow/java/resources.md): `FlowResource`, регистрацию фабрик и `RuntimeContext.getResource`. В Kotlin действуют те же правила жизненного цикла и привязки в спецификации.

## Как Kotlin работает во Flow {#architecture}

Kotlin-компьютейшены исполняются через механизм [компаньонов](../../flow/concepts/glossary.md#companion): [воркер](../../flow/concepts/glossary.md#worker) запускает JVM-процесс с вашим кодом и обменивается с ним сообщениями по gRPC. Спека использует тот же Java-компаньон, что и Java, а параметр `main_class` обязателен всегда.

JVM-раннер выбирает окружение джобы и доставляет jar-файлы компаньона только при включённом Vanilla-запуске. В docker-джобе `jdk_bin_path` имеет приоритет над `YT_FLOW_JDK_BIN_PATH`, а затем используется путь к JVM, на которой работает лаунчер. Выбранный путь должен существовать в образе джобы и указывать на совместимую версию Java. В porto-режиме с JDK-слоем от лаунчера переменная `YT_FLOW_JDK_BIN_PATH` может переопределить путь из слоя, а отличающийся `jdk_bin_path` в ресурсе игнорируется. Если воркер объявляет собственные слои, JDK-слой лаунчера не используется: задайте совместимый путь в `jdk_bin_path` или `YT_FLOW_JDK_BIN_PATH`. Не задавайте `classpath` для jar-файлов, которые должен доставить Vanilla-лаунчер. Заданный classpath сохраняется, а в смешанной спеке jar-файлы по-прежнему доставляются для ресурсов без `classpath`. При прямом деплое{% if audience == "internal" %}, например через Deploy / YP,{% endif %} такого обогащения спеки нет: JDK, jar-файлы и все параметры ресурса компаньона должны быть доступны в окружении воркера. См. [Vanilla](../../flow/devops/vanilla/initial-deploy.md) и [docker](../../flow/devops/docker-environment.md). Роли Runner и Worker описаны в разделе [Архитектура приложения (Java)](../../flow/java/getting-started.md#architecture).

## Сборка с ya make {#build}

Kotlin-пайплайн собирается модулем `JAVA_PROGRAM` с включённым компилятором Kotlin. Полный `ya.make` примера [Word Count]({{source-root}}/yt/yt/flow/examples/kotlin/word_count) подключается из исходников:

{% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/ya.make' lang='text' %}

`WITH_KOTLIN()` подключает компилятор Kotlin, а `WITH_KOTLINC_ALLOPEN(preset=spring)` открывает классы с Spring-аннотациями, которые Kotlin по умолчанию делает `final`. Собрать пайплайн и `flow_server` можно одной командой:

```bash
cd yt/yt/flow
ya make examples/kotlin/word_count/wordcount bin/flow_server
```

Для модуля `JAVA_PROGRAM(wordcount_kotlin_bin)` сборка создаёт `run.sh`, программный jar-файл `wordcount_kotlin_bin.jar`, jar-файлы зависимостей в каталоге `wordcount_kotlin_bin/` и архив `jdk.tar`. Модуль собирается для JDK 21, а макрос `WITH_JDK()` упаковывает эту JDK в `jdk.tar`, но не распаковывает и не развёртывает автоматически каталог `jdk/`. На хосте запуска предоставьте Java 21 через `JAVA_HOME` или передайте путь к бинарю `java` начальной опцией `run.sh -j <путь/к/java>`. Опция `-j` имеет приоритет над `JAVA_HOME`. Отдельный docker-пример с образом `flow-java` использует Java 17, поэтому не рассчитывайте, что этот образ запустит модуль для JDK 21: выбранный путь или образ джобы должен предоставлять совместимую среду выполнения.

Сохраните взаимное расположение `run.sh`, `wordcount_kotlin_bin.jar` и каталога `wordcount_kotlin_bin/` и запускайте скрипт из содержащего их каталога: пути к classpath и JDK отсчитываются от текущего каталога, а сам скрипт не переходит в каталог своего файла. В сгенерированном `run.sh` не задан главный класс, поэтому первым аргументом после необязательной начальной опции `-j` передайте то же полное имя класса точки входа, что и в `main_class` ресурса компаньона.

При включённом Vanilla-запуске не задавайте `classpath` в параметрах ресурса компаньона, если раннер должен доставить эти jar-файлы. Заданный `classpath` сохраняется и означает, что jar-файлы ресурса уже есть в окружении джобы. Если `classpath` задан у всех ресурсов Java-компаньона, раннер не доставляет jar-файлы; в смешанной спеке он по-прежнему доставляет их для ресурсов без `classpath`.

## Точка входа {#entry-point}

Точка входа — класс с JVM-статическим методом `main`. В Kotlin статический `main` объявляется через `@JvmStatic` в `object` или в `companion object`:

{% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/WordCountApplication.kt' lang='kotlin' lines='[BEGIN word_count_application]-[END word_count_application]' keep-indents %}

В параметре `main_class` указывается полное имя этого класса. Для Kotlin Word Count это `tech.ytsaurus.flow.examples.wordcount.WordCountApplication`. Top-level `fun main` компилируется в класс с суффиксом `Kt`, поэтому `object` или `companion object` с `@JvmStatic` позволяют сохранить ожидаемое имя класса.

## Запуск пайплайна {#launch}

Запускайте команду из каталога с `run.sh`, программным jar-файлом и каталогом зависимостей. Первый аргумент после необязательной начальной опции `-j <путь/к/java>` — полное имя класса точки входа: его получает `run.sh`. Остальные аргументы получает раннер. Параметр `--config` обязателен; `--flow-bin` можно не указывать, если путь к `flow_server` задан в `YT_FLOW_BIN`:

```bash
./run.sh tech.ytsaurus.flow.examples.wordcount.WordCountApplication --config pipeline.yson --flow-bin <путь/к/flow_server>
```

Запуск в vanilla-операции описан на странице [Запуск пайплайна в Vanilla-операции](../../flow/devops/vanilla/initial-deploy.md), а в docker-окружении — в [Запуске в docker-окружении](../../flow/devops/docker-environment.md).{% if audience == "internal" %} Для Deploy / YP см. [Запуск пайплайна через Deploy / YP](../../flow/devops/deploy/initial-deploy.md).{% endif %}

## Nullability {#nullability}

Java SDK размечен аннотациями [JSpecify](https://jspecify.dev/). Kotlin 2.1+ применяет их строго: `@Nullable`-результат становится `T?`, а nullable-значение нельзя передать в non-null параметр. Kotlin-примеры проверяют эти аннотации в CI.

## Тестирование {#testing}

Юнит-тесты Kotlin process-функций используют тот же `flow-test-utils`, что и Java-тесты; см. [Тестирование (Java)](../../flow/java/testing.md). Тестовый модуль объявляется через `JUNIT6()` с `WITH_KOTLIN()`; пример — [`src/test/ya.make`]({{source-root}}/yt/yt/flow/examples/kotlin/word_count/wordcount/src/test/ya.make). Интеграционные тесты примеров используют тот же класс точки входа, что и ресурс компаньона.

## Примеры {#examples}

Kotlin-исходники доступны для [Word Count]({{source-root}}/yt/yt/flow/examples/kotlin/word_count), [Shuffle]({{source-root}}/yt/yt/flow/examples/kotlin/shuffle), [Async Request]({{source-root}}/yt/yt/flow/examples/kotlin/async_request), [Retryable Async Request]({{source-root}}/yt/yt/flow/examples/kotlin/retryable_async_request), [Wait Click Join]({{source-root}}/yt/yt/flow/examples/kotlin/wait_click_join), [URL Downloader]({{source-root}}/yt/yt/flow/examples/kotlin/url_downloader), [Static Table Join]({{source-root}}/yt/yt/flow/examples/kotlin/static_table_join) и [External State Join]({{source-root}}/yt/yt/flow/examples/kotlin/external_state_join). Соответствующие объяснения API приведены в разделе Java.

{% if audience == "internal" %}

Для Logbroker есть Kotlin-пример [`lb_wait_click_join`]({{source-root}}/yt/yt/flow/yandex/extensions/logbroker/examples/kotlin/lb_wait_click_join).

{% endif %}

## См. также

- [Быстрый старт (Java)](../../flow/java/getting-started.md)
- [Computation (Java)](../../flow/java/computation.md)
- [Spring Boot интеграция](../../flow/java/spring.md)
- [Companion](../../flow/concepts/companion.md)
