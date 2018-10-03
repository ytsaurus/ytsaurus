BIN="/home/max42/yt_arc/build/ytserver-clickhouse"
VERSION="$($BIN --version)"
CYPRESS_PATH="//sys/clickhouse/bin/ytserver-clickhouse-$VERSION"
echo "Deploying $BIN of version $VERSION to $CYPRESS_PATH"
cat $BIN | pv | yt write-file $CYPRESS_PATH
yt set $CYPRESS_PATH/@executable "%true"
