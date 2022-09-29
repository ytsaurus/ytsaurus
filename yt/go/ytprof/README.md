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

### Pprof UI

* `https://ytprof.yt.yandex-team.ru/ui/{profile_id}/`: entry point to pprof UI for given profile
Example: `curl https://ytprof.yt.yandex-team.ru/ui/5b9e57db-630103fe-7b30799-8760a484/`

### HTTP Requests

* `https://ytprof.yt.yandex-team.ru/api/get`: get profile by ProfileID in the format of guid (type GET)
Example: `curl https://ytprof.yt.yandex-team.ru/api/get?profile_id=e2c699bb-4d58862c-f741fb63-6c2ed55a`
* `https://ytprof.yt.yandex-team.ru/api/list`: find metadata specified by query (type POST)
Example: `curl -X POST -d '{"metaquery":{"time_period":{"period_start_time":"2022-04-24T00:00:00.000000Z","period_end_time":"2022-04-29T00:00:00.000000Z"},"metadata_pattern":{"host":"sas6.*node.*freud."}}}' https://ytprof.yt.yandex-team.ru/api/list`
Here in `metadata_pattern`, usage of [regexp](https://pkg.go.dev/regexp#MatchString) is supported.
* `https://ytprof.yt.yandex-team.ru/api/suggest_tags`: get all possible tags (type GET)
Example: `curl https://ytprof.yt.yandex-team.ru/api/suggest_tags`
* `https://ytprof.yt.yandex-team.ru/api/suggest_values`: get all possible values of a tag (type GET)
Example: `curl https://ytprof.yt.yandex-team.ru/api/suggest_values?tag=ArcRevision`
* `https://ytprof.yt.yandex-team.ru/api/merge`: find and merge profiles by ProfileIDs in the format of guids (type GET)
Example: `curl https://ytprof.yt.yandex-team.ru/api/merge?profile_ids=92699db6-200114a4-dd451d4e-9e12204&profile_ids=44a94bf4-a6420e9b-67d0f83f-2a295c48`
* `https://ytprof.yt.yandex-team.ru/api/merge_all`: find and merge profiles specified by the same query as `list` (type POST)
Example: `curl -X POST -d '{"metaquery":{"time_period":{"period_start_time":"2022-04-24T00:00:00.000000Z","period_end_time":"2022-04-29T00:00:00.000000Z"},"metadata_pattern":{"host":"sas6.*node.*freud."}}}' https://ytprof.yt.yandex-team.ru/api/merge_all`

See `requsts` and `responces` [here](https://a.yandex-team.ru/arcadia/yt/go/ytprof/api/api.proto).

### Run Service

Here is an instruction how to run API & UI sevice localy from `arcadia/yt/go/ytprof` directory:

Build
```
ya make -r ./cmd/ytprof-api
```

Run
```
./cmd/ytprof-api/ytprof-api --log-to-stderr --config-json '{"http_endpoint": "0.0.0.0:8080", "proxy": "freud", "table_path": "//sys/ytprof/testing"}'
```
