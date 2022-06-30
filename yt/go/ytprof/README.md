# Ytprof
API for storing and finding profiles and their metadata & more

## Contents

* [Command Line](#s-CommandLine)
* [API](#s-API)

## <a name="s-CommandLine"></a> Command Line

Here is an instruction how to directly run ytprof commands localy from `arcadia/yt/go/ytprof` directory:

Build
```
ya make -r ./cmd/cmd
```

Help

```
./cmd/cmd/cmd --help
```

## <a name="s-API"></a> API

Here is a [link](https://nanny.yandex-team.ru/ui/#/services/catalog/yt_ytprof) to the nanny service.

### HTTP Requsts

* `/ytpfof/api/get`: get profile by ProfileID in the format of guid (type GET)
* `/ytpfof/api/list`: find metadata specified by query (type POST)
* `/ytpfof/api/suggest_tags`: get all possible tags (type GET)
* `/ytpfof/api/suggest_values`: get all possible values of a tag (type GET)

See `requsts` and `responces` [here](https://a.yandex-team.ru/arcadia/yt/go/ytprof/api/api.proto).

### Run Service

Here is an instruction how to run API sevice localy from `arcadia/yt/go/ytprof` directory:

Build
```
ya make -r ./cmd/ytprof-api
```

Run
```
./cmd/ytprof-api/ytprof-api --log-to-stderr --config-json '{"http_endpoint": "0.0.0.0:8080", "proxy": "freud", "table_path": "//sys/ytprof/testing"}'
```
