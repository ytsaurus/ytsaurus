# AGENTS.md

## Branch naming

When creating a git branch for a change, it must be named as `members/{username}/{short-change-description}`.

## Style guides

Added C++ and Python code should follow the repository style guides in `yt/styleguide/styleguide.md` and its linked language-specific documents.

## Remote build and test workflow

Use `ya_remote.sh` from the repository root to sync the repository to the `yt-cloud-dev` virtual machine and run `ya` remotely.

Basic build command:

```bash
bash ya_remote.sh --target yt/yt/server/all
```

This script:

- syncs the current repository to `yt-cloud-dev`
- uses the remote repository directory `~/ytsaurus/ytsaurus` by default
- runs `./ya make` remotely

## Build examples

Build the default server-side target from `BUILD.md`:

```bash
bash ya_remote.sh
```

Build an explicit target:

```bash
bash ya_remote.sh --target yt/yt/server/all
```

Build with a custom job count:

```bash
bash ya_remote.sh --target yt/yt/server/all --jobs 32
```

## Test types

There are 2 types of tests in this repository:

1. Unittests – they normally live in the `unittests` directory and can be identified by `ya.make` content with the `GTEST()` module.
2. Integration tests – they live inside `yt/yt/tests/integration`. Always specify the exact test name when running them.

## Test rules

When running tests with this repository and this script, follow these rules strictly:

1. Never run all tests.
2. Always specify the test directory explicitly.
3. Always use the flag `--run-all-tests` when running tests.
4. For integration tests, always specify an explicit test filter with `-F "*{test_name}*"`.
5. For tests in `yt/python/yt/wrapper/tests`, always run only the exact target test or tests needed for your change, the filter could be used `-F "*{test_name}*"`; do not run the whole wrapper test suite.
6. After implementing any feature or fix, always run a build and the appropriate tests before considering the work complete.
 7. Any new functionality or functional change must include a test. Prefer unittests when practical; if it is unclear whether unittest or integration coverage is more appropriate, ask the user which kind of test should be added.

## Test examples

Run unittests for an explicit directory:

```bash
bash ya_remote.sh --target yt/yt/core/rpc/unittests --test-size all
```

Run an explicit integration test only:

```bash
bash ya_remote.sh --target yt/yt/tests/integration/scheduler --test-size all -- -F "*test_name*"
```

Run an explicit Python wrapper test only:

```bash
bash ya_remote.sh --target yt/python/yt/wrapper/tests --test-size all -- --run-all-tests test_dynamic_table_commands.py::TestDynamicTables::test_insert_lookup_delete_with_transaction
```

## Notes

- Do not invoke the script without an explicit target when your intent is to run tests.
- Do not run repository-wide test commands.
- Put extra `ya` arguments after `--` so they are passed through to `./ya make` on the remote host.

## Python wrapper client generation

- If public methods in `yt/python/yt/wrapper/client_api.py` (or methods they export) change, regenerate `YtClient` implementation in `yt/python/yt/wrapper/client_impl.py`.
- Use the generated binary from target `yt/python/yt/wrapper/bin/generate_client_impl` and run it with output path `yt/python/yt/wrapper/client_impl.py`.
- Do not hand-edit generated `client_impl.py`; re-run the generator instead.
