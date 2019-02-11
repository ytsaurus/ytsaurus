echo "Fetching available binaries..."
yt find --type file //sys/clickhouse/bin -l | tr -s ' ' | sort -k4,5 -r
echo ""

if [[ "$(yt exists //sys/clickhouse/bin/ytserver-clickhouse)" == "true" ]] ; then
    current_target="$(yt get --format dsv '//sys/clickhouse/bin/ytserver-clickhouse&/@target_path')"
else
    current_target="(none)"
fi

echo -e "Current link: //sys/clickhouse/bin/ytserver-clickhouse -> ${current_target}\n"

read -p "Suggest filename without any extra tokens, quotes, etc:
" name

CYPRESS_PATH_LINK="//sys/clickhouse/bin/ytserver-clickhouse"
yt link --force "$name" "$CYPRESS_PATH_LINK"
