kind=yt-start-clickhouse-clique

if [[ "$1" != "" ]] ; then
    kind=$1
fi

if [[ "$kind" != "yt-start-clickhouse-clique" ]] ; then
    echo "Wrong kind ${kind}, expected one of: yt-start-clickhouse-clique"
    exit 1
fi

echo "Fetching available binaries..."
yt find --type file //sys/bin/${kind} -l | tr -s ' ' | sort -k4,5 -r | grep linux | sed -s "s/linux/PLATFORM/g"
echo ""

read -p "Suggest filename without any extra tokens, quotes, etc:
" name

for platform in linux darwin windows ; do
    if [[ "$platform" == "windows" ]] ; then 
        ext=".exe"
    else
        ext=""
    fi

    link_path="//sys/bin/${kind}/${kind}.${platform}${ext}"
    yt link --force "${name/PLATFORM/${platform}}$ext" "$link_path"
done
