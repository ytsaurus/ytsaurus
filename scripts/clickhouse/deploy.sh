if [[ "$1" == "" ]] ; then
    BIN="/home/max42/yt/build-rel/bin/ytserver-clickhouse"
else
    BIN="$1"
fi
VERSION="$($BIN --version)"
CYPRESS_PATH="//sys/clickhouse/bin/ytserver-clickhouse-$VERSION"
echo "Deploying $BIN of version $VERSION to $CYPRESS_PATH"
yt write-file $CYPRESS_PATH < $BIN
yt set $CYPRESS_PATH/@executable "%true"
yt set $CYPRESS_PATH/@version "\"$VERSION\""
