# Ytprof
API for storing and finding profiles and their metadata & more

## Contents

* [Command Line](#s-CommandLine)
* [API](#s-API)

## <a name="s-CommandLine"></a> Command Line

Here is an instruction how to directly run ytprof commands locally from arcadia root:

### Build
```
ya make -r ./yt/go/ytprof/cmd/cmd
```

### Help
```
./yt/go/ytprof/cmd/cmd/cmd --help
```

### Example of using manual storage
First step is to build:
```
ya make -r ./yt/go/ytprof/cmd/cmd
```

Now if you have a cpu profile in `./cpu.prof` you want to view or/and save:
```
./yt/go/ytprof/cmd/cmd/cmd push --file ./cpu.prof --type cpu --name "I_can_name_it!"
```
Result:
```
Link to view your profile:
https://ytprof.yt.yandex-team.ru/manual/ui/7f178d72-a1a7381-dee75c0b-dcf54b5c/
```

In case you lost a link or want to find some profiles you can use metaquery command:
```
./yt/go/ytprof/cmd/cmd/cmd list --stats --last 10h --link --metaquery "Metadata['Name'].matches('^I.can.*t.$')"
```
Result:
```
GUID:7f178d72-a1a7381-dee75c0b-dcf54b5c Timestamp:2022-12-13T09:56:41 ArcRevision:b63f925fe3f01719d017920dc988f16639a9e1b4 BinaryVersion:22.4.0-local-ya~b63f925fe3f01719+achulkov2 Cluster:none Host:none ProfileType:cpu Service:none Name:I_can_name_it! 
https://ytprof.yt.yandex-team.ru/manual/ui/7f178d72-a1a7381-dee75c0b-dcf54b5c/
GUID:9c45c275-2c68c505-aceaa2ea-4fddd736 Timestamp:2022-12-13T10:09:37 ArcRevision:b63f925fe3f01719d017920dc988f16639a9e1b4 BinaryVersion:22.4.0-local-ya~b63f925fe3f01719+achulkov2 Cluster:test Host:none ProfileType:cpu Service:none Name:I_can't_name_it! 
https://ytprof.yt.yandex-team.ru/manual/ui/9c45c275-2c68c505-aceaa2ea-4fddd736/
2	 - Total
2	 - Total of Service
	2	 - of none.
2	 - Total of BinaryVersion
	2	 - of 22.4.0-local-ya~b63f925fe3f01719+achulkov2.
2	 - Total of ArcRevision
	2	 - of b63f925fe3f01719d017920dc988f16639a9e1b4.
2	 - Total of Name
	1	 - of I_can_name_it!.
	1	 - of I_can't_name_it!.
2	 - Total of ProfileType
	2	 - of cpu.
2	 - Total of Host
	2	 - of none.
2	 - Total of Cluster
	1	 - of test.
	1	 - of none.
```
If not every profile has tag `Name` you will encounter this error `{"level":"fatal","ts":"2022-12-13T14:36:46.381+0300","caller":"cmd/metaquery.go:209","msg":"metaquery failed","error":"no such key: Name"}`, to fix that use `--ignore-errors` option.

## <a name="s-API"></a> API

Here is a [link](https://nanny.yandex-team.ru/ui/#/services/catalog/yt_ytprof) to the nanny service.

### Pprof UI

* `https://ytprof.yt.yandex-team.ru/{system}/ui/{profile_id}/`: entry point to pprof UI for given profile
Example: `curl https://ytprof.yt.yandex-team.ru/yt/ui/5b9e57db-630103fe-7b30799-8760a484/`

### HTTP Requests

* `https://ytprof.yt.yandex-team.ru/api/systems`: get available systems  (type GET)
Example: `curl https://ytprof.yt.yandex-team.ru/api/systems`
* `https://ytprof.yt.yandex-team.ru/api/get`: get profile by ProfileID in the format of guid (type GET)
Example: `curl https://ytprof.yt.yandex-team.ru/api/get?system=yt&profile_id=e2c699bb-4d58862c-f741fb63-6c2ed55a`
* `https://ytprof.yt.yandex-team.ru/api/list`: find metadata specified by query (type POST)
Example: `curl -X POST -d '{"metaquery":{"system":"yt","time_period":{"period_start_time":"2022-04-24T00:00:00.000000Z","period_end_time":"2022-04-29T00:00:00.000000Z"},"metadata_pattern":{"host":"sas6.*node.*freud."}}}' https://ytprof.yt.yandex-team.ru/api/list`
Here in `metadata_pattern`, usage of [regexp](https://pkg.go.dev/regexp#MatchString) is supported.
* `https://ytprof.yt.yandex-team.ru/api/suggest_tags`: get all possible tags (type GET)
Example: `curl https://ytprof.yt.yandex-team.ru/api/suggest_tags?system=yt`
* `https://ytprof.yt.yandex-team.ru/api/suggest_values`: get all possible values of a tag (type GET)
Example: `curl https://ytprof.yt.yandex-team.ru/api/suggest_values?system=yt&tag=ArcRevision`
* `https://ytprof.yt.yandex-team.ru/api/merge`: find and merge profiles by ProfileIDs in the format of guids (type GET)
Example: `curl https://ytprof.yt.yandex-team.ru/api/merge?system=yt&profile_ids=92699db6-200114a4-dd451d4e-9e12204&profile_ids=44a94bf4-a6420e9b-67d0f83f-2a295c48`
* `https://ytprof.yt.yandex-team.ru/api/merge_link`: find, merge profiles by ProfileIDs in the format of guids and store result in manual storage, receive a link to view stored profile (type GET)
Example: `curl https://ytprof.yt.yandex-team.ru/api/merge_link?system=yt&profile_ids=92699db6-200114a4-dd451d4e-9e12204&profile_ids=44a94bf4-a6420e9b-67d0f83f-2a295c48`
* `https://ytprof.yt.yandex-team.ru/api/merge_all`: find and merge profiles specified by the same query as `list` (type POST)
Example: `curl -X POST -d '{"metaquery":{"system":"yt","time_period":{"period_start_time":"2022-04-24T00:00:00.000000Z","period_end_time":"2022-04-29T00:00:00.000000Z"},"metadata_pattern":{"host":"sas6.*node.*freud."}}}' https://ytprof.yt.yandex-team.ru/api/merge_all`

See `requests` and `responses` [here](https://a.yandex-team.ru/arcadia/yt/go/ytprof/api/api.proto).

### Run Service

Here is an instruction how to run API & UI service locally from arcadia root directory:

Build
```
ya make -r ./yt/go/ytprof/cmd/ytprof-api
```

Run
```
./yt/go/ytprof/cmd/ytprof-api/ytprof-api --log-to-stderr --config-json '{"http_endpoint": ":10033","proxy":"hahn","folder_path":"//home/ytprof/storage","query_limit":200000}'
```
