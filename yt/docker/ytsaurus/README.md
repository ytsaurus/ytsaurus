## Build image

# Prepare

Take binaries and other files from SBR:4224358132 or make them yourself:

1. Build cpp binaries `ytserver-all`, `ytserver-clickhouse` (from build directory where `cmake` was run):
```
ninja ytserver-all
ninja ytserver-clickhouse
```

2. Build go binary with `chyt-controller`:
```
cd yt/chyt/controller/cmd/chyt-controller
go build .
```

3. Generate or find somewhere CREDITS files:
- ytserver-all: run in arcadia `ya make -r -DOPENSOURCE --with-credits yt/yt/server/all` and find ytserver-all.CREDITS.txt file in `yt/yt/server/all`;

4. Build python package with driver bindings (TODO: add normal instruction)
SBR: 4218783248
`python3 setup.py bdist_wheel --universal`
or take built from dist/

# Build docker image

`./build.sh`

