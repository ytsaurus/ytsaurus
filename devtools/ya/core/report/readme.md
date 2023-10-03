# Отправка информации в snowden
## Общая информация
Библиотека предоставляет возможность отправлять отладочную информацию о различных событиях в [snowden](https://abc.yandex-team.ru/services/snowden), конкретно [сюда](https://yt.yandex-team.ru/hahn/navigation?sort=asc-false,field-name&path=//home/devtools-snowden/snowden_v1_report).
В каждом сообщении, помимо типа сообщения `ReportTypes` и значения (которое может быть любым json-сериализуемым объектом python) отправляются:
- `_id`: Уникальный id сообщения
- `hostname`: Информация о имени компьютера в текущей сети
- `user`: Имя пользователя
- `platform_name`: Информация о системе
- `session_id`: Уникальный id сессии (в одну сессию попадают все сообщения из одного запуска ya)
- `namespace`: 
- `key`: Тип сообщения
- `value`: Сообщение
- `timestamp`: Время системы

## [EXECUTION](https://a.yandex-team.ru/search?search=ReportTypes%5C.EXECUTION,,j,arcadia,,200&repo=arc_vcs)
Информация о запуске ya: командная строка, переменные окружения, текущий рабочий каталог.  
Отправляется **только один раз** в начале работы!

```json
{
    "env_vars": {
      "ENV_KEY": "ENV_VALUE"
    },
    "cmd_args": [
      "/path/to/ya-bin",
      "handler",
      "sub_handlers",
      "arguments",
      "--options",
    ],
    "cwd": "",
    "__file__": "devtools/ya/app/__init__.py"
}
```

## [RUN_YMAKE](https://a.yandex-team.ru/search?search=ReportTypes%5C.RUN_YMAKE,,j,arcadia,,200&repo=arc_vcs)
Информация о запуске утилиты ymake.  
Включает в себя уникальный id запуска ymake, цель, с которой запускался ymake (например для построения тулов), используемые кеши, различные метрики и статистика исполнения, код возврата и ошибка.
Отправляется **после каждого** успешного или неуспешного исполнения ymake.

```json
{
    "binary": "/path/to/ymake",
    "stats": {
        "execution": {
            "duration": 12.781816959381104,
            "start": 1634724201.58872,
            "finish": 1634724214.370537
        },
        "preparing": {
            "duration": 0.0325770378112793,
            "start": 1634724201.556132,
            "finish": 1634724201.588709
        },
        "postprocessing": {
            "duration": 0.0002841949462890625,
            "start": 1634724214.37054,
            "finish": 1634724214.370824
        }
    },
    "ymake_run_uid": "10fd7152-f27c-42db-9da1-b56c2c1bb212",
    "args": [
        "/Users/v-korovin/.ya/tools/v3/1103068819/ymake",
        "arguments",
        "--options"
    ],
    "exit_code": 0,
    "metrics": {
        "Internal cache": {
            "Diagnostics cache size on save": 80,
            "Modules table size on save": 477224,
            "Parsers cache size on save": 808752,
            "Times table size on save": 16,
            "Names table size on save": 9419984,
            "Graph cache size on save": 2535552,
            "Total cache size on save": 13241840
        },
        "File access": {
            "mapped MD5 time": 1066925,
            "loaded count": 20390,
            "loaded MD5 time": 27754,
            "Max mapped MD5 time": 79763,
            "loaded size": 127132946,
            "mapped size": 201296536,
            "load time": 4010276,
            "max load time": 6461,
            "map time": 39180,
            "FromPatchSize": 0,
            "mapped count": 493,
            "FromPatchCount": 0,
            "Max loaded MD5 time": 30
        },
        "Parsing": {
            ".in files size": 5324009,
            "parsed files recovered": 0,
            "parsed files size": 258642410,
            ".in files count": 36,
            "parsed files count": 17333,
            "parse time": 581199
        },
        "DepGraph": {
            "nodes count": 79163,
            "edges count": 315521,
            "commands count": 34131,
            "files count": 60189
        },
        "ya.make parsing": {
            "count": 1299
        },
        "UpdIter": {
            "nuke mod dir": 0
        },
        "TModules": {
            "accessed": 1024,
            "total": 1938,
            "outdated": 0,
            "parsed": 1938,
            "loaded": 0
        }
    },
    "purpose": "default-darwin-x86_64-release-nopic",
    "duration": 12.781816959381104,
    "stages": {
        "Sort edges": {
            "duration": 0.1365349292755127,
            "start": 1634724212.702975,
            "finish": 1634724212.83951
        },
        "Other Stages": {
          "duration": 0.02682805061340332 ,
          "start": 1634724212.985625 ,
          "finish": 1634724213.012453
        }
    },
    "caches": {
        "Deps cache": {
            "loaded": false,
            "saved": true
        },
        "FS cache": {
            "loaded": false,
            "saved": true
        }
    }
}

```

## [FAILURE](https://a.yandex-team.ru/search?search=ReportTypes%5C.FAILURE,,j,arcadia,,200&repo=arc_vcs)
Информация о необработанном исключении во время работы ya.    
Включает в себя параметры запуска командной строки, переменные окружения, текущий рабочий каталог, тип исключения и форматированный traceback.  
Отправялется **только в случае необработанного исключения** в конце работы ya!

```json
{
    "retriable": null,
    "mute": null,
    "type": "BdbQuit",
    "env_vars": {},
    "traceback": "Full traceback text from format_exc()",
    "prefix": [
        "ya",
        "handler",
        "subhandlers"
    ],
    "__file__": "devtools/ya/app/__init__.py",
    "cmd_args": [],
    "cwd": ""
}

```

## [TIMEIT](https://a.yandex-team.ru/search?search=ReportTypes%5C.TIMEIT,,j,arcadia,,200&repo=arc_vcs)
Метрики работы ya — память, операции ввода/вывода, время работы, был ли запуск успешным.
Отправляется **только один раз** в конце работы ya.

```json
{
    "success": true,
    "inblock": 0,
    "prefix": [],
    "max_rss": 306768,
    "stime": 2.849071,
    "duration": 42.02627491950989,
    "oublock": 0,
    "cmd_args": [],
    "cwd": "",
    "utime": 5.123886
}

```

## [LOCAL_YMAKE](https://a.yandex-team.ru/search?search=ReportTypes%5C.LOCAL_YMAKE,,j,arcadia,,200&repo=arc_vcs)
Путь и контент локального конфигурационного ymake-файла (дополнеие к ymake.core.conf)

## [HANDLER](https://a.yandex-team.ru/search?search=ReportTypes%5C.HANDLER,,j,arcadia,,200&repo=arc_vcs)
Информация об аргументах командной строки при определении handler-а

```json
{
    "prefix": [],
    "args": [
        "arguments",
        "--options",
    ]
}

```

## [DIAGNOSTICS](https://a.yandex-team.ru/search?search=ReportTypes%5C.DIAGNOSTICS,,j,arcadia,,200&repo=arc_vcs)
url диагностических данных при включенном флаге `--diag`:
- Логи
- Конфигурации
- json графы
- Кеши ymake

```json
{
  "url": "link://to.sandbox/resource"
}
```

## [WTF_ERRORS](https://a.yandex-team.ru/search?search=ReportTypes%5C.WTF_ERRORS,,j,arcadia,,200&repo=arc_vcs)
Неожиданные ошибки

## [PROFILE_BY_TYPE](https://a.yandex-team.ru/search?search=ReportTypes%5C.PROFILE_BY_TYPE,,j,arcadia,,200&repo=arc_vcs)
Статистика исполнения сборочного графа:
- Суммарное время работы подзадач и их количество
- Полное время работы
- Время и количество подзадач по типам
- Критический путь с таймингами
- Флаги
- Сборочные цели
- Количество потоков

```json
{
    "all": {
        "sum": 41.256314516067505,
        "qty": 1083
    },
    "wall_time": 32.26124310493469,
    "critical_path": [
        {
            "timing": [
                1634724170.461328,
                1634724170.468779
            ],
            "type": "pattern[PYTHON]",
            "name": "Pattern(PYTHON)"
        },
        {}, {}
    ],
    "rel_targets": [
        "path/to/project"
    ],
    "threads": 7,
    "by_type": {
        "pattern[MACOS_SDK-sbr:2088833948]": {
            "sum": 0.009443044662475586,
            "qty": 1
        },
        "other[types]": {
            "sum": 0.019350051879882812,
            "qty": 1
        },
    },
    "flags": {
        "CONSISTENT_DEBUG_LIGHT": "yes"
    },
    "build_type": "release"
}

```

## [PLATFORMS](https://a.yandex-team.ru/search?search=ReportTypes%5C.PLATFORMS,,j,arcadia,,200&repo=arc_vcs)
Информация о host- и target- платформах

```json
{
    "host": {
        "platform": {
            "host": {
                "visible_name": "clang12",
                "arch": "x86_64",
                "os": "DARWIN",
                "toolchain": "default"
            },
            "target": {
                "visible_name": "clang12",
                "arch": "x86_64",
                "os": "DARWIN",
                "toolchain": "default"
            }
        },
        "name": "clang12"
    },
    "targets": [
        {
            "platform": {
                "host": {
                    "visible_name": "clang12",
                    "arch": "x86_64",
                    "os": "DARWIN",
                    "toolchain": "default"
                },
                "target": {
                    "visible_name": "clang12",
                    "arch": "x86_64",
                    "os": "DARWIN",
                    "toolchain": "default"
                }
            },
            "name": "clang12"
        }
    ]
}

```

## [VCS_INFO](https://a.yandex-team.ru/search?search=ReportTypes%5C.VCS_INFO,,j,arcadia,,200&repo=arc_vcs)
Информация об используемой vcs

```json
{
    "BUILD_DATE": "2021-10-20T10:02:39.000000Z",
    "ARCADIA_PATCH_NUMBER": 8746134,
    "ARCADIA_SOURCE_PATH": "",
    "BUILD_TIMESTAMP": 1634724159,
    "ARCADIA_SOURCE_HG_HASH": "330044e4582fbbce1b0a160cf26e0aee962e5434",
    "ARCADIA_SOURCE_URL": "",
    "ARCADIA_TAG": "",
    "ARCADIA_SOURCE_LAST_AUTHOR": "arcadia-devtools",
    "BUILD_HOST": "v-korovin-osx",
    "DIRTY": "dirty",
    "BRANCH": "trunk",
    "BUILD_USER": "v-korovin",
    "ARCADIA_SOURCE_REVISION": 8746134,
    "ARCADIA_SOURCE_LAST_CHANGE": 8746134,
    "VCS": "arc"
}
```


## [PARAMETERES](https://a.yandex-team.ru/search?search=ReportTypes%5C.PARAMETERS,,j,arcadia,,200&repo=arc_vcs)
Информация об обработанных параметрах запуска ya

```json
{
    "parameter_name": "parameter_value",
    ...
}
```
