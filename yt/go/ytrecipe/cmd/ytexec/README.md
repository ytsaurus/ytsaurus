# ytexec

```
ytexec --config config.json
```

`ytexec` прозрачно уносит исполнение команды внутрь vanilla операции на
кластере YT.

Команда принимает один параметр - путь до конфига. Поля конфига описаны в [godoc](https://godoc.yandex-team.ru/pkg/a.yandex-team.ru/yt/go/ytrecipe/internal/ytexec/#Config)

**Note:** `time.Duration` кодируется в json как время в наносекундах.

В случае успеха команда выходит с кодом 0. В случае внутренней ошибки,
команда выходит с кодом != 0 и пишет ошибку в stderr.

## Пример конфига

```json
{
    "exec": {
        "prepared_file": "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/testing_out_stuff/prepare.json",
        "result_file": "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/testing_out_stuff/result.json",
        "readme_file": "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/testing_out_stuff/README.md",
        "download_script": "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/testing_out_stuff/download.sh",
        "exec_log": "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/testing_out_stuff/ytexec.log",
        "job_log": "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/testing_out_stuff/job.log",
        "yt_token_env": ""
    },
    "operation": {
        "cluster": "freud",
        "pool": "yt-integration-tests",
        "title": "[TS] yt/go/ytrecipe/test/gotest [0/1]",
        "cypress_root": "//home/yt-integration-tests",
        "output_ttl": 3600000000000,
        "blob_ttl": 3600000000000,
        "coordinate_upload": true,
        "cpu_limit": 0,
        "memory_limit": 536870912,
        "timeout": 3600000000000,
        "enable_porto": true,
        "enable_network": false,
        "spec_patch": null,
        "task_patch": null
    },
    "cmd": {
        "args": [
            "/home/prime/.ya/tools/v4/1555203013/test_tool",
            "run_go_test",
            "--binary",
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/gotest",
            "--tracefile",
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/ytest.report.trace",
            "--modulo",
            "1",
            "--modulo-index",
            "0",
            "--partition-mode",
            "SEQUENTIAL",
            "--output-dir",
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/testing_out_stuff",
            "--project-path",
            "yt/go/ytrecipe/test/gotest",
            "--verbose"
        ],
        "cwd": "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest",
        "environ": [
            "ARCADIA_TESTS_DATA_DIR=/home/prime/.ya/build/build_root/x674/000162/environment/arcadia_tests_data",
            "PY_IGNORE_ENVIRONMENT=",
            "YA_SOURCE_ROOT=/home/prime/Code/go/src/a.yandex-team.ru",
            "YA_CC=/home/prime/.ya/tools/v4/958916803/bin/clang",
            "LC_NUMERIC=ru_RU.UTF-8",
            "ASAN_SYMBOLIZER_PATH=/home/prime/.ya/tools/v4/958916803/bin/llvm-symbolizer",
            "TESTING_SAVE_OUTPUT=yes",
            "ORIGINAL_SOURCE_ROOT=/home/prime/Code/go/src/a.yandex-team.ru",
            "PYTHONDONTWRITEBYTECODE=1",
            "XDG_CURRENT_DESKTOP=GNOME",
            "XDG_SESSION_TYPE=x11",
            "QT_IM_MODULE=ibus",
            "LOGNAME=prime",
            "USER=prime",
            "YA_CXX=/home/prime/.ya/tools/v4/958916803/bin/clang++",
            "HOME=/home/prime",
            "ARCADIA_BUILD_ROOT=/home/prime/.ya/build/build_root/x674/000162",
            "SSH_AGENT_PID=3660",
            "XDG_SESSION_DESKTOP=gnome-xorg",
            "YTRECIPE_CONFIG_PATH=/home/prime/.ya/build/build_root/x674/000162/environment/arcadia/yt/go/ytrecipe/test/gotest/ytrecipe.yson",
            "YA_TC=1",
            "LSAN_OPTIONS=exitcode=100",
            "ARCADIA_ROOT_DISTBUILD=/home/prime/Code/go/src/a.yandex-team.ru",
            "TEST_WORK_PATH=/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest",
            "WINEPREFIX=/home/prime/.ya/build/build_root/x674/000001",
            "PORT_SYNC_PATH=/home/prime/.ya/build/port_sync_dir",
            "TMPDIR=/home/prime/.ya/tmp/1592053374.151336.hbwk2ibe",
            "GORACE=halt_on_error=1",
            "GOPATH=/home/prime/Code/go",
            "YA_SPLIT_INDEX=0",
            "USERNAME=prime",
            "LANG=en",
            "XDG_RUNTIME_DIR=/run/user/1000",
            "SSH_AUTH_SOCK=/run/user/1000/keyring/ssh",
            "YA_SPLIT_COUNT=0",
            "VTE_VERSION=6003",
            "ARCADIA_SOURCE_ROOT=/home/prime/.ya/build/build_root/x674/000162/environment/arcadia",
            "GDMSESSION=gnome-xorg",
            "XMODIFIERS=@im=ibus",
            "TEST_COMMAND_WRAPPER=/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/cmd/jobrun/jobrun",
            "MSAN_SYMBOLIZER_PATH=/home/prime/.ya/tools/v4/958916803/bin/llvm-symbolizer",
            "TEST_NAME=gotest",
            "LC_ALL=C.UTF-8",
            "DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus",
            "WINDOWPATH=2",
            "MANAGERPID=3524",
            "DESKTOP_SESSION=gnome-xorg",
            "DISPLAY=:0",
            "LSAN_SYMBOLIZER_PATH=/home/prime/.ya/tools/v4/958916803/bin/llvm-symbolizer",
            "TSAN_OPTIONS=external_symbolizer_path=/home/prime/.ya/tools/v4/958916803/bin/llvm-symbolizer",
            "MSAN_OPTIONS=exitcode=100",
            "UBSAN_SYMBOLIZER_PATH=/home/prime/.ya/tools/v4/958916803/bin/llvm-symbolizer",
            "GDM_LANG=en_US.UTF-8",
            "LC_MEASUREMENT=ru_RU.UTF-8",
            "YA_TEST_RUNNER=1",
            "PWD=/home/prime/Code/go/src/a.yandex-team.ru",
            "XDG_SESSION_CLASS=user",
            "LC_PAPER=ru_RU.UTF-8",
            "XDG_MENU_PREFIX=gnome-",
            "LC_TIME=ru_RU.UTF-8",
            "INVOCATION_ID=72869db5b5434d7d9852ab647ccf2ec4",
            "XDG_DATA_DIRS=/home/prime/.local/share/flatpak/exports/share/:/var/lib/flatpak/exports/share/:/usr/local/share/:/usr/share/",
            "ASAN_OPTIONS=exitcode=100",
            "YA_GLOBAL_RESOURCES={\"GO_TOOLS_RESOURCE_GLOBAL\": \"/home/prime/.ya/tools/v4/1539496897\", \"YOLINT_RESOURCE_GLOBAL\": \"/home/prime/.ya/tools/v4/1547017150\", \"LLD_ROOT_RESOURCE_GLOBAL\": \"/home/prime/.ya/tools/v4/1063258680\", \"YOLINT_NEXT_RESOURCE_GLOBAL\": \"/home/prime/.ya/tools/v4/1547017150\", \"OS_SDK_ROOT_RESOURCE_GLOBAL\": \"/home/prime/.ya/tools/v4/244387436\"}"
        ],
        "sigusr2_timeout": 0,
        "sigquit_timeout": 60000000000,
        "sigkill_timeout": 61000000000
    },
    "fs": {
        "upload_file": [
            "/home/prime/.ya/tools/v4/1555203013/test_tool",
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/gotest"
        ],
        "upload_tar": null,
        "upload_structure": [
            "/home/prime/.ya/build/build_root/x674/000162",
            "/home/prime/.ya/tmp/1592053374.151336.hbwk2ibe",
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest"
        ],
        "outputs": [
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/ytest.report.trace",
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/testing_out_stuff"
        ],
        "stdout_file": "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/testing_out_stuff/stdout",
        "stderr_file": "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/testing_out_stuff/stderr",
        "yt_outputs": [
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/ytrecipe_output"
        ],
        "coredump_dir": "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/ytrecipe_coredumps",
        "ext4_dirs": [
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/ytrecipe_hdd"
        ],
        "download": {
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/testing_out_stuff": "testing_out_stuff",
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/ytrecipe_coredumps": "ytrecipe_coredumps",
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/ytrecipe_hdd": "ytrecipe_hdd",
            "/home/prime/.ya/build/build_root/x674/000162/yt/go/ytrecipe/test/gotest/test-results/gotest/ytrecipe_output": "ytrecipe_output"
        }
    }
}
```
