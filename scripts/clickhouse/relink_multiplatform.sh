kind=ytserver-clickhouse

if [[ "$1" != "" ]] ; then
    kind=$1
fi

if [[ "$kind" != "yt-start-clickhouse-clique" ]] ; then
    echo "Wrong kind ${kind}, expected one of: yt-start-clickhouse-clique"
    exit 1
fi

echo "Fetching available binaries..."
yt find --type file //sys/clickhouse/bin -l | tr -s ' ' | sort -k4,5 -r | grep $kind | grep linux | sed -s "s/linux/PLATFORM/g"
echo ""

read -p "Suggest filename without any extra tokens, quotes, etc:
" name

for platform in linux darwin windows ; do
    if [[ "$platform" == "windows" ]] ; then 
        ext=".exe"
    else
        ext=""
    fi

    CYPRESS_PATH_LINK="//sys/clickhouse/bin/${kind}.${platform}${ext}"
    yt link --force "${name/PLATFORM/${platform}}$ext" "$CYPRESS_PATH_LINK"
done
