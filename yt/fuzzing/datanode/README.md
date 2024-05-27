## How to run datanode fuzz test

1. Set `YT_REPO_PATH` env var
2. Collect corpus:

    a. Build ytserver-all. Set `ENABLE_DUMP_PROTO_MESSAGE` CMake flag.
    ```
    build_dir $ cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_TOOLCHAIN_FILE=$YT_REPO_PATH/clang.toolchain -DENABLE_DUMP_PROTO_MESSAGE=1 $YT_REPO_PATH 
    build_dir $ ninja ytserver-all
     ```

    b. Run integration tests. Corpus will be written to `/tmp/datanode_corpus_merged_reqs_with_attachments`
    ```
    BULD_DIR_PATH $ ninja convert_operations_to_binary_format scheduler_simulator libyson_lib.so libdriver_lib.so libdriver_rpc_lib.so
    rm -rf $HOME/yt/python
    bash yt/yt/scripts/prepare_pytest_environment.sh
    bash yt/yt/scripts/run_integration_tests.sh --ytsaurus-source-path $(pwd) --ytsaurus-build-path $BULD_DIR_PATH
    ```
3. Start master process (compile without fuzzer instrumentation; do not set ENABLE_DUMP_PROTO_MESSAGE)
```bash
ytserver-master --config $YT_REPO_PATH/yt/fuzzing/datanode/master.yson
```
4. Build fuzz test. Set `ENABLE_FUZZER` and `DISABLE_YT_VERIFY` CMake flags
```bash
build_fuzz $ cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_TOOLCHAIN_FILE=../ytsaurus/clang.toolchain -DENABLE_FUZZER=1 -DDISABLE_YT_VERIFY=1 ../ytsaurus
build_fuzz $ ninja fuzztests-datanode
```
5. Run fuzz test
```bash
build_fuzz $ ./yt/fuzzing/datanode/fuzztests-datanode /tmp/new_inputs_datanode /tmp/datanode_corpus_merged_reqs_with_attachments -artifact_prefix=/tmp/fuzzing_artifacts_datanode/ -fork=5 -ignore_crashes=1 -rss_limit_mb=20000
```

Crashes are saved in `/tmp/datanode_corpus_merged_reqs_with_attachments`.

Inputs revealing new code paths are saved in `/tmp/new_inputs_datanode`.

6. Reproduce found crashes.
- Build `reproducer`
```bash
build_dir $ cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DDISABLE_YT_VERIFY=1 -DCMAKE_TOOLCHAIN_FILE=$YT_REPO_PATH/clang.toolchain -DENABLE_DUMP_PROTO_MESSAGE=1 $YT_REPO_PATH 
build_dir $ ninja reproducer
```
- Run data node and replay requests that cause crashes
```bash
build_dir $ ./yt/fuzzing/reproducer/reproducer /tmp/fuzzing_artifacts_datanode/<crash id>
```

7. Print fuzzer input in a human-readable format
```bash
build_dir $ ./yt/fuzzing/protobuf-reader/protobuf-reader /tmp/fuzzing_artifacts_datanode/crash-e23435d3d2e44d9cae108e7e484e45147fc3fe37
```

Print methods of unique requests from sample input files.
```bash
build_dir $ for file in /tmp/fuzzing_artifacts_datanode/*; do echo "Processing $file:"; ./yt/fuzzing/protobuf-reader/protobuf-reader "$file" | head -n 1; done
```

## Fuzz test description

The Datanode is launched once before any tests are launched. The state of the Datanode is not reset between individual tests
