BIN="/home/max42/yt/build-rel/bin/ytserver-clickhouse"
VERSION="$($BIN --version)"
CYPRESS_PATH="//sys/clickhouse/bin/ytserver-clickhouse-$VERSION"
echo "Deploying $BIN of version $VERSION to $CYPRESS_PATH"
cat $BIN | pv | yt write-file $CYPRESS_PATH
yt set $CYPRESS_PATH/@executable "%true"
yt set $CYPRESS_PATH/@version "\"$VERSION\""
