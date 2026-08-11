# Flow pipeline testing framework

This page describes how to set up a workstation and configure low-level parameters for contributors who are improving Flow itself and running its integration test suite. If you're a pipeline author testing your code, see [Testing (C++)](../../../flow/cpp/testing.md), [Testing (Java)](../../../flow/java/testing.md), and [Testing (Python)](../../../flow/python/testing.md) — these pages include the launch parameters and techniques you need for pipeline tests.

## Environment setup {#setup}

The Flow test suite captures Flow process crashes and extracts backtraces from coredumps. For this to work, you must enable coredump collection on your machine:

```bash
# Temporary (it’s better to add this to ~/.bashrc).
ulimit -c unlimited
```

Also, add the process PID to the coredump filename:

```bash
# Temporary — valid until reboot:
sudo sysctl -w kernel.core_uses_pid=1

# Permanent:
echo "kernel.core_uses_pid=1" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

Without `core_uses_pid=1`, you might see odd behavior, such as extracting multiple backtraces from a single coredump.

If you need the UI of the local {{product-name}} for debugging, you’ll need network access — see the [relevant section](../../../flow/cpp/testing.md#debug-ui-yt) in the documentation for pipeline authors.

## Flow test parameters {#test-param}

In addition to the general parameters (see [Testing (C++)](../../../flow/cpp/testing.md#debug-test-framework)), these parameters are useful when you’re working on Flow itself — they affect the framework’s internal behavior:

#|
|| **Parameter** | **Default** | **Values** | **Action** ||
|| `DUMP_BACKTRACES_IF_HANGS` | `0` | `0`, `1` | Write backtraces of threads and fibers from running Flow processes to disk when an unhandled `WaitFailed` occurs during shutdown. ||
|| `FLOW_BINARY_PATH` | | Absolute path | Use a fixed pipeline binary — for example, one built with a thread sanitizer. ||
|| `ERROR_BACKTRACE_ENRICHER_LEVEL` | `enabled_for_not_native_errors` | [see enum]({{source-root}}/yt/yt/flow/cpp/misc/error_backtrace_enricher.h) | Controls whether backtraces are added to errors in the logs. ||
|#

Examples:

```bash
ya make -A --test-param ERROR_BACKTRACE_ENRICHER_LEVEL=enabled_for_all
ya make -A --test-param DUMP_BACKTRACES_IF_HANGS=1
```