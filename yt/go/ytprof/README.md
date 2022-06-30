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
Example: `curl <url>/ytprof/api/get?profile_id=e2c699bb-4d58862c-f741fb63-6c2ed55a`
* `/ytpfof/api/list`: find metadata specified by query (type POST)
Example: `curl -X POST -d '{"metaquery":{"time_period":{"period_start_time":"2022-04-24T00:00:00.000000Z","period_end_time":"2022-04-29T00:00:00.000000Z"},"metadata_pattern":{"host":"sas6.*node.*freud."}}}' <url>/ytprof/api/list`
Here in `metadata_pattern`, usage of [regexp](https://pkg.go.dev/regexp#MatchString) is supported.
* `/ytpfof/api/suggest_tags`: get all possible tags (type GET)
Example: `curl <url>/ytprof/api/suggest_tags`
* `/ytpfof/api/suggest_values`: get all possible values of a tag (type GET)
Example: `curl <url>/ytprof/api/suggest_values?tag=ArcRevision`
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
