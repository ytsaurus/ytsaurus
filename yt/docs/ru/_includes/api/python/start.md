## Python API

{% note info "Примечание" %}

Перед началом работы установите Python-клиент из pip-репозитория следующей командой:

```bash
pip install ytsaurus-client
```

{% endnote %}

После установки пакета становится доступным:

- Python библиотека yt;
- Бинарный файл [yt](../../../api/cli/cli.md);
- Бинарный файл yt-fuse для подключения [Кипариса](../../../user-guide/storage/cypress.md) в качестве файловой системы локально.

### Установка { #install }

#### Библиотеки YSON

Для использования YSON формата для работы с таблицами потребуются C++ биндинги, которые устанавливаются отдельным пакетом. Установка [YSON биндингов](../../../api/python/userdoc.md#yson_bindings):

   ```bash
   pip install ytsaurus-yson
   ```

{% note warning "Внимание" %}

В настоящий момент нет возможности установить YSON биндинги под Windows.

{% endnote %}

{% note info "Для пользователей платформы Apple M1" %}

В настоящий момент нет YSON биндингов собранных под платформу Apple. В качестве временного решения можно воспользоваться [Rosetta 2](https://ru.wikipedia.org/wiki/Rosetta_(программное_обеспечение)) и установить версию Python для архитектуры x86_64.

Подробнее об этом можно прочитать по [ссылке](https://stackoverflow.com/questions/71691598/how-to-run-python-as-x86-with-rosetta2-on-arm-macos-machine).


{% endnote %}

Подробнее про YSON можно прочитать в разделе [Форматы](../../../api/python/userdoc.md#formats).

Чтобы узнать версию установленной обертки из Python, распечатайте переменную `yt.VERSION` или вызовите команду `yt --version`.

При возникновении проблем ознакомьтесь с разделом [FAQ](#faq). Если проблема сохранилась, напишите в [чат](https://t.me/ytsaurus_ru).

[Исходный код библиотеки](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/yt/wrapper).

{% note warning "Внимание" %}

Не рекомендуется устанавливать библиотеку и зависимые от нее пакеты разными способами одновременно. Это может приводить к трудно диагностируемым проблемам.

{% endnote %}

### Документация для пользователей { #userdoc }
  * [Общее](../../../api/python/userdoc.md#common)
    - [Соглашения, использующиеся в коде](../../../api/python/userdoc.md#agreements)
    - [Клиент](../../../api/python/userdoc.md#client)
      - [Потокобезопасность](../../../api/python/userdoc.md#threadsafety)
      - [Асинхронный клиента на основе gevent](../../../api/python/userdoc.md#gevent)
    - [Конфигурация](../../../api/python/userdoc.md#configuration)
      - [Общий конфиг](../../../api/python/userdoc.md#configuration_common)
      - [Настройка логирования](../../../api/python/userdoc.md#configuration_logging)
      - [Настройка токена](../../../api/python/userdoc.md#configuration_token)
      - [Настройка ретраев](../../../api/python/userdoc.md#configuration_retries)
    - [Ошибки](../../../api/python/userdoc.md#errors)
    - [Форматы](../../../api/python/userdoc.md#formats)
    - [YPath](../../../api/python/userdoc.md#ypath)
  * [Команды](../../../api/python/userdoc.md#commands)
    - [Работа с кипарисом](../../../api/python/userdoc.md#cypress_commands)
    - [Работа с файлами](../../../api/python/userdoc.md#file_commands)
    - [Работа с таблицами](../../../api/python/userdoc.md#table_commands)
      - [Датаклассы](../../../api/python/userdoc.md#dataclass)
      - [Схемы]( ../../../api/python/userdoc.md#table_schema)
      - [TablePath](../../../api/python/userdoc.md#tablepath_class)
      - [Команды](../../../api/python/userdoc.md#table_commands)
      - [Параллельное чтение таблиц](../../../api/python/userdoc.md#parallel_read)
      - [Параллельная запись таблиц](../../../api/python/userdoc.md#parallel_write)
    - [Работа с транзакциями и локами](../../../api/python/userdoc.md#transaction_commands)
    - [Запуск операций](../../../api/python/userdoc.md#run_operation_commands)
      - [SpecBuilder](../../../api/python/userdoc.md#spec_builder)
    - [Работа с операциями и джобами](../../../api/python/userdoc.md#operation_and_job_commands)
      - [Operation](../../../api/python/userdoc.md#operation_class)
      - [OperationsTracker](../../../api/python/userdoc.md#operations_tracker_class)
      - [OperationsTrackerPool](../../../api/python/userdoc.md#operations_tracker_pool_class)
    - [Работы с правами доступа](../../../api/python/userdoc.md#acl_commands)
    - [Работа с динамическими таблицами](../../../api/python/userdoc.md#dyntables_commands)
    - [Другие команды](../../../api/python/userdoc.md#etc_commands)
  * [Python-объекты в качестве операций](../../../api/python/userdoc.md#python_operations)
    - [Общая информация](../../../api/python/userdoc.md#python_operations_intro)
    - [Подготовка операции из джоба](../../../api/python/userdoc.md#prepare_operation)
    - [Декораторы](../../../api/python/userdoc.md#python_decorators)
    - [Pickling функции и окружения](../../../api/python/userdoc.md#pickling)
      - [Общее устройство](../../../api/python/userdoc.md#pickling_description)
      - [Ссылка на пост с советами](../../../api/python/userdoc.md#pickling_advises)
    - [Porto-слои](../../../api/python/userdoc.md#porto_layers)
    - [tmpfs в джобах](../../../api/python/userdoc.md#tmpfs_in_jobs)
    - [Статистики в джобах](../../../api/python/userdoc.md#python_jobs_statistics)
  * [Нетипизированные Python-операции](../../../api/python/userdoc.md#python_operations_untyped)
    - [Декораторы](../../../api/python/userdoc.md#python_decorators_untyped)
    - [Форматы](../../../api/python/userdoc.md#python_formats)
       - [Структурированное представление данных](../../../api/python/userdoc.md#structured_data)
       - [Контрольные атрибуты](../../../api/python/userdoc.md#control_attributes)
       - [Другие форматы](../../../api/python/userdoc.md#other_formats)
  * [Другое](../../../api/python/userdoc.md#other)
    - [gRPC](../../../api/python/userdoc.md#grpc)
    - [YSON-биндинги](../../../api/python/userdoc.md#yson_bindings)
  * [Устаревшее](../../../api/python/userdoc.md#legacy)
    - [Python3 и байтовые строки](../../../api/python/userdoc.md#python3_strings)

### Справка { #pydoc }
Самая актуальная справка по конкретным функциям и их параметрам находится в коде.

Посмотреть описание функций и классов в интерпретаторе можно следующим образом:

```bash
$ python
>>> import yt.wrapper as yt
>>> help(yt.run_sort)
```

### Примеры { #examples }

  * [Базовый уровень](../../../api/python/examples.md#base)
    - [Чтение и запись таблиц](../../../api/python/examples.md#read_write)
    - [Схемы таблиц](../../../api/python/examples.md#table_schema)
    - [Простой map](../../../api/python/examples.md#simple_map)
    - [Сортировка таблицы и простая операция reduce](../../../api/python/examples.md#sort_and_reduce)
    - [Reduce с несколькими входными таблицами](../../../api/python/examples.md#reduce_multiple_output)
    - [Reduce с несколькими входными и несколькими выходными таблицами](../../../api/python/examples.md#reduce_multiple_input_output)
    - [MapReduce](../../../api/python/examples.md#map_reduce)
    - [MapReduce с несколькими промежуточными таблицами](../../../api/python/examples.md#map_reduce_multiple_intermediate_streams)
    - [Декораторы для классов-джобов](../../../api/python/examples.md#job_decorators)
    - [Работа с файлами на клиенте и в операциях](../../../api/python/examples.md#files)
    - [Генеричный grep](../../../api/python/examples.md#grep)
  * [Продвинутый уровень](../../../api/python/examples.md#advanced)
    - [Batch запросы](../../../api/python/examples.md#batch_queries)
    - [RPC](../../../api/python/examples.md#rpc)
  * [Разное](../../../api/python/examples.md#misc)
    - [Датаклассы](../../../api/python/examples.md#dataclass)
    - [Контекст и управление записью в выходные таблицы ](../../../api/python/examples.md#table_switches)
    - [Spec builders](../../../api/python/examples.md#spec_builder)
    - [Использование gevent](../../../api/python/examples.md#gevent)

<!-- ### Для разработчика { #fordeveloper }

  * [Контрибы](for_developer.md#contribs)
  * [Разбиение библиотеки на части в Аркадии](for_developer.md#peerdirs)
  * [Устройство и запуск тестов](for_developer.md#tests)
  * [Политика обновления библиотеки](for_developer.md#update_policy) -->

### FAQ { #faq }

В данном разделе собраны ответы на ряд частых вопросов, касающихся Python API. Ответы на другие частые вопросы в разделе [FAQ](../../../faq/faq.md).

**Q: Установил пакет через pypi, но получаю ошибку `yt: command not found`.**
A: Попробуйте выполнить команду
`pip install ytsaurus-client --force-reinstall`
скорее всего в логе будет warning вида `The script yt is installed in '...' which isn't on your PATH`. Для решения проблемы необходимо добавить указанный путь в переменную окружения PATH. Для этого нужно выполнить следующую команду:

```
echo 'export PATH="$PATH:<указанный путь>"' >> ~/.bashrc
source ~/.bashrc
```
В зависимости от оболочки файл может называться по-другому. Чаще всего на Mac он называется `~/.zshrc`.

**Q: Чтение с retry завершается ошибкой из-за превышения таймаута.**
A: Скорее всего в таблице слишком много чанков, нужно укрупнить их. Используйте `yt merge --src table --dst table --spec "{combine_chunks=true}"`

**Q: Операция завершается с ошибкой YSON-а (например: `YsonError: Premature end of stream`), а в веб-интерфейсе появляется ошибка парсинга  YSON.**
A: Скорее всего, операция пишет в `stdout`. Это запрещено делать явно в Python через `print, sys.stdout.write()`, если операция не помечена как `raw_io`, но это может делать сторонняя программа, например, архиватор.

**Q: Python библиотека слишком много пишет в stderr, как повысить уровень логирования?**
A: Уровень можно повысить, установив переменную окружения `YT_LOG_LEVEL="ERROR"`, или через настройку логгера {{product-name}}: `logging.getLogger("Yt").setLevel(logging.ERROR)`.

**Q: Запускаю операцию с Mac OS X, а джобы завершаются с ошибками типа `ImportError: ./tmpfs/modules/_ctypes.so: invalid ELF header`.**
A: Так как Python wrapper забирает с собой на кластер все зависимости Python операции, то туда же приезжают бинарные .so и .pyc файлы, которые потом не могут быть загружены. Стоит использовать porto-слой с вашим локальным окружением, а также включить фильтрацию этих файлов, чтобы они не попадали на кластер. Подробнее можно прочитать в [разделе](../../../api/python/userdoc.md#porto_layers).

**Q: Джобы завершаются с ошибкой `Invalid table index N: expected integer in range [A,B]`.**
A: Сообщение означает, что в записях вы выдаете table index, причем соответствующей таблицы нет. Чаще всего это означает, что у вас несколько входных таблиц, а выходная таблица одна. Во входных записях по умолчанию приходят поля `@table_index`, чтобы их выключить, можно поменять формат: `yt.config["tabular_data_format"] = yt.YsonFormat(process_table_index=None)`. Подробнее про формат можно прочитать в [разделе](../../../api/python/userdoc.md#python_formats)). В качестве альтернативы явно укажите в спецификации (пример для map-операции):  `{"mapper": {"enable_input_table_index": False}}`.

**Q: При запуске операции, после того, как она стала completed, появляется ошибка (ReadTimeout, HTTPConnectionPool(....): Read timed out.).**
A: Сообщение означает, что не удалось скачать stderr операции из-за сетевых проблем, причём не помогли даже повторные запросы. В таком случае, стоит воспользоваться опцией `ignore_stderr_if_download_failed`, которая позволяет игнорировать stderr, если его не удалось скачать. Рекомендуется использовать опцию при написании production-процессов.

**Q: Получаю ошибку `Yson bindings required`.**
A: Это означает, что в качестве входного (выходного) формата выбран YSON и в джобе не удалось импортировать биндинги. Подробнее про YSON и биндинги к нему можно прочитать в [разделе](../../../api/python/userdoc.md#yson). Нужно установить пакет с биндингами, а также проверить, что YSON биндинги не отфильтровываются с помощью `module_filter`. Это динамическая библиотека yson_lib.so, и ее можно легко нечаянно отфильтровать, если отфильтровывать все .so файлы. Кроме того, чтобы `yt_yson_bindings`, приехавшие в модулях, не удалялись, в файле конфигурации нужно прописать `config["pickling"]["ignore_yson_bindings_for_incompatible_platforms"] = False`.


