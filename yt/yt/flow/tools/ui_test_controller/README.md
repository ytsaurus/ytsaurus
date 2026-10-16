# Flow UI test controller

`ui_test_controller` is a read-only controller endpoint for deterministic UI tests. It replays a FlowView and job orchids captured by the live fixture pipeline under `fixture_pipeline`, attaches a snapshot runtime to the production command executor, and registers the production `ControllerService` RPC service. It contains no command-specific response generation.

Generate fresh fixtures by running the live integration test:

```bash
ya make -A yt/yt/flow/tools/ui_test_controller/fixture_pipeline
```

The test output `flow_ui_fixtures` contains the captured FlowViews, job orchids, scenario manifest,
and replay clock. Runtime paths, worker hostnames, addresses, and build versions are normalized before
writing the YSON. The capture fails if it finds secret-like fields, token formats, or values from
sensitive environment variables. The Ya output files are symlinks, so the archive must dereference them
and normalize its headers:

```bash
tar --zstd --dereference --sort=name --mtime='UTC 1970-01-01' \
    --owner=0 --group=0 --numeric-owner \
    -cf flow-ui-live-fixtures.tar.zst -C "$fixture_dir" .
```

Upload the resulting archive as a Sandbox resource with infinite TTL.

Start the controller:

```bash
ya make
./ui_test_controller \
    --flow-view /tmp/working_healthy.yson \
    --job-orchids /tmp/working_healthy-job-orchids.yson \
    --pipeline-path '<cluster=ui>//tmp/flow-ui-screenshot/working_healthy/pipeline' \
    --now-seconds 1789570034 \
    --port 19010
```

The surrounding test harness is responsible for creating a local pipeline, publishing the RPC address as `leader_controller_address`, and provisioning the `flow_control` row used by the UI header.

Canonical fixtures are captured artifacts stored in infinite-TTL Sandbox resource [`13809993962`](https://sandbox.yandex-team.ru/resource/13809993962/view), owned by `YT_FLOW`. Change the live test pipeline or its lifecycle harness, rerun the capture target, upload its complete output as a replacement resource, and review the resulting describe/UI changes; do not edit fixture YSON by hand.
