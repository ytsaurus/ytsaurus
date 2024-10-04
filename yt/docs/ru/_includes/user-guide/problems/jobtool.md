# Локальная отладка джобов

Для удобства отладки джобов (например, под GDB) была написана утилита `yt job-tool`, которая скачивает окружение джоба, его входные данные, а также формирует скрипт для его запуска.

Утилита поставляется вместе с пакетом [Python API](../../../api/python/start.md). Поддерживаются все типы джобов, в которых запускается пользовательский код.

Бинарный файл поддерживает две команды: `prepare-job-environment` и `run-job`. Для получения подробной информации используйте команду `yt job-tool --help`.

## Входные данные для джоба

По умолчанию `job-tool` получает полный вход джоба, но он доступен не для всех джобов:

* Спецификация, необходимая для получения полного входа, сохраняется для нескольких упавших и нескольких успешно отработавших джобов, а также доступна для всех ещё бегущих джобов.
* Для получения полного входа должны быть доступны данные, которые читал джоб, т. е. входные таблицы должны быть на месте и в неизменённом виде.
* Джобы стадий Reduce_combiner и Reduce операции MapReduce читают временные данные, которые доступны, только пока операция не завершилась. Если требуется отладка таких джобов, операцию можно переписать на Map + Sort + Reduce или выполнять отладку, пока операция не завершена (в том числе пока операция в состоянии suspended).

## Пример

1. Запускаем следующую операцию:

    ```python
    import yt.wrapper as yt

    def mapper(rec):
        raise RuntimeError("fail")

    if __name__ == "__main__":
        yt.run_map(mapper, "//home/user/tables/dsv", "//home/user/output", format="dsv")
    ```

2. Когда операция упала, диагностируем:

    ```bash
    yt job-tool prepare-job-environment ffc8f462-7f68f8c3-3fe03e8-433fe11f ffe4ff5c-d2fac8c1-3fe0384-816a7fd0
    ```

3. После того как команда отработает, в папке `job_ffe4ff5c-d2fac8c1-3fe0384-816a7fd0` будут лежать следующие файлы:

    ```bash
    $ ls job_ffe4ff5c-d2fac8c1-3fe0384-816a7fd0
    command  input  run_gdb.sh  run.sh  sandbox
    ```

    Где:

    * Файл `command` содержит команду для запуска джоба. Так как операция была запущена с помощью [Python API](../../../api/python/start.md), код будет иметь вид:

      ```bash
      cat job_ffe4ff5c-d2fac8c1-3fe0384-816a7fd0/command
      python _py_runner.py mapper.lV4l9c config_dump6RRYa4 _modulesEFMotR _main_moduleJALUsC.py _main_module PY_SOURCE%
      ```

    * `input` — вход джоба, в некоторых случаях может быть полезен для анализа.
    * `run.sh` — shell-скрипт, запускающий джоб в локальном режиме.
    * `run_gdb.sh` — shell-скрипт, запускающий джоб под gdb, что полезно для отладки C++ программ.
    
      {% note info %}

      Скрипты `run_gdb.sh`, `run.sh` можно модифицировать, например, чтобы вставить свой отладчик.

      {% endnote %}

    * `sandbox` — директория, в которой запускался джоб, со всеми необходимыми для его запуска файлами.
      Файлы в директории можно пробовать обновлять в рамках отладки.


4. Запускаем джоб локально с помощью скрипта `run.sh`:

    ```bash
    ./run
    2016-07-22 12:00:33,499 INFO    Started job process
    User job exited with non-zero exit code 1 with stderr:
    Traceback (most recent call last):
      File "_py_runner.py", line 56, in <module>
        main()
      File "_py_runner.py", line 53, in main
        yt.wrapper.py_runner_helpers.process_rows(__operation_dump_filename, __config_dump_filename, start_time=start_time)
      File "/home/user/yt/python/yt/wrapper/py_runner_helpers.py", line 154, in process_rows
        output_format.dump_rows(result, streams.get_original_stdout(), raw=raw)
      File "/home/user/yt/python/yt/wrapper/format.py", line 137, in dump_rows
        self._dump_rows(rows, stream)
      File "/home/user/yt/python/yt/wrapper/format.py", line 251, in _dump_rows
        for row in rows:
      File "/home/user/yt/python/yt/wrapper/py_runner_helpers.py", line 89, in process_frozen_dict
        for row in rows:
      File "/home/user/yt/python/yt/wrapper/py_runner_helpers.py", line 49, in generator
        result = func(*args)
      File "<stdin>", line 2, in mapper
    RuntimeError: fail

        origin          hostname in 2016-07-22T12:00:35.427161Z
    ```




