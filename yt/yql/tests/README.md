# How to run tests with YQL

1. Select an image of Query Tracker to use YQL artifacts from. For example,
```bash
export IMAGE=ghcr.io/ytsaurus/query-tracker-nightly:dev-2024-07-13-a4d9cf430666575a7aaf17d6f9890000c42e0a25-relwithdebinfo
```

2. Extract image contents into some directory, say, `~/qt`.
```bash
mkdir ~/qt
cd ~/qt
docker pull $IMAGE
ID=$(docker create $IMAGE false)
docker export $ID -o image.tar
docker rm $ID
```

3. Move `mrjob` binary to the directory with YQL UDFs.
```bash
mv usr/bin/mrjob usr/lib/yql
```

4. Set `YDB_ARTIFACTS_PATH` environment variable to the location of the artifacts.
```bash
export YDB_ARTIFACTS_PATH=~/qt/usr/lib/yql
```

5. Run tests with `ya make`.

# Modifying YQL code

1. Clone YDB repository
```bash
git clone https://github.com/ydb-platform/ydb
```

2. Modify code and build required artifacts. For example, `libyqlplugin.so` is built from `ydb/library/yql/yt/dynamic` and `mrjob` is built from `ydb/library/yql/tools/mrjob`.

3. Copy newly built artifact to the artifact directory (`~/qt/usr/lib/yql` in the example above).

4. Run tests with `ya make`.
