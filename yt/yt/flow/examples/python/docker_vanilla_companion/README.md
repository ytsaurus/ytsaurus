# docker_vanilla_companion

A Python companion that runs in an image the pipeline chooses, instead of the cluster's default job
environment.

`reader` (`TSwiftPassthroughOrderedSourceComputation` over `TQueueSource`) → `mapper`
(`TTransformCompanionComputation`, `text_mapper.py`) → the `mapped` stream. The mapper copies its
input columns through and adds `text_upper`, computed in Python.

## Job image

`docker_image` is per task, so the spec sets it on both the controller and the worker:

```yson
"controller" = {"count" = 1; "docker_image" = "registry.example.com/ytflow-python-companion:tag";};
"worker" = {"count" = 1; "docker_image" = "registry.example.com/ytflow-python-companion:tag"; ...};
```

The companion is then spawned by the image's own interpreter, and the only job file left is the
user's code:

```yson
"entrypoint" = {"executable" = "/usr/local/bin/python3"; "args" = ["main.py"];};
```

## Build the image

`yt/yt/flow/tools/python_companion_package/Dockerfile` builds the base image — an interpreter with
the companion SDK and `ytsaurus-client` installed. Build it from the checkout root so the SDK is
compiled from the same sources as the `flow_server` it will talk to:

```bash
docker build -f yt/yt/flow/tools/python_companion_package/Dockerfile \
    -t registry.example.com/ytflow-python-companion:tag .
docker push registry.example.com/ytflow-python-companion:tag
```

This example needs nothing beyond the SDK, so it uses that image as it is and ships `main.py` as a
job file. A computation that imports third-party packages should instead inherit from it —
`FROM registry.example.com/ytflow-python-companion:tag` — add what it needs, and drop the
`local_files` entry.

A private registry needs credentials: YT reads them from a secure-vault entry named `docker_auth`,
which the runner fills from an environment variable of the same name when the spec asks for it with
`"secret_env" = ["docker_auth"]`.

## Run

Fill in the cluster coordinates, pool, queue paths, image and the path to `main.py` in
`pipeline.yson`, then submit it with the runner:

```bash
flow_server --config pipeline.yson
```
