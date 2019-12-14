kind=ytserver-clickhouse

if [[ "$1" != "" ]] ; then
    kind=$1
fi

if [[ "$kind" != "ytserver-log-tailer" && "$kind" != "ytserver-clickhouse" && "$kind" != "clickhouse-trampoline" ]] ; then
    echo "Wrong kind ${kind}, expected one of: ytserver-log-tailer, ytserver-clickhouse, clickhouse-trampoline"
    exit 1
fi

echo "Fetching available binaries..."
yt find --type file //sys/clickhouse/bin -l | tr -s ' ' | sort -k4,5 -r | grep $kind
echo ""

if [[ "$(yt exists //sys/clickhouse/bin/${kind})" == "true" ]] ; then
    path="//sys/clickhouse/bin/${kind}&/@target_path"
    cmd="yt get --format dsv ${path}"
    current_target=$($cmd)
else
    current_target="(none)"
fi

echo -e "Current link: //sys/clickhouse/bin/${kind} -> ${current_target}\n"

read -p "Suggest filename without any extra tokens, quotes, etc:
" name

CYPRESS_PATH_LINK="//sys/clickhouse/bin/${kind}"
yt link --force "$name" "$CYPRESS_PATH_LINK"
