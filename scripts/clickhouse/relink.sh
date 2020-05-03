kind=ytserver-clickhouse

if [[ "$1" != "" ]] ; then
    kind=$1
fi

if [[ "$kind" != "ytserver-log-tailer" && "$kind" != "ytserver-clickhouse" && "$kind" != "clickhouse-trampoline" ]] ; then
    echo "Wrong kind ${kind}, expected one of: ytserver-log-tailer, ytserver-clickhouse, clickhouse-trampoline"
    exit 1
fi

echo "Fetching available binaries..."
yt find --type file //sys/bin/${kind} -l | tr -s ' ' | sort -k4,5 -r 
echo ""

link_path="//sys/bin/${kind}/${kind}"

if [[ "$(yt exists ${link_path})" == "true" ]] ; then
    path="${link_path}&/@target_path"
    cmd="yt get --format dsv ${path}"
    current_target=$($cmd)
else
    current_target="(none)"
fi

echo -e "Current link: ${link_path} -> ${current_target}\n"

read -p "Suggest filename without any extra tokens, quotes, etc:
" name

yt link --force "$name" "$link_path"
