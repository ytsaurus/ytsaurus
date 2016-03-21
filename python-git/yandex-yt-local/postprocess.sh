#!/bin/bash -ex
# Upload archive with all necessary stuff to Locke and Sandbox.

export YT_PROXY=locke.yt.yandex.net

# Tokens belong to teamcity@ user
export YT_TOKEN=1da6afc98d189e8ba59d2ea39f29d0f1
export SANDBOX_TOKEN=4420f146b4bf4829b809ae1cbbfabbfc

export PYTHONPATH="$(pwd)"

YT="$(pwd)/yt/wrapper/yt"

UBUNTU_CODENAME=$(lsb_release -c -s)

sandbox_request() {
    local method="$1" && shift
    local path="$1" && shift
    curl -X "$method" -sS -k -L "https://sandbox.yandex-team.ru/api/v1.0/${path}" \
         -H "Content-Type: application/json" \
         -H "Authorization: OAuth $SANDBOX_TOKEN" \
         "$@"
}

strip_debug_info() {
    local archive_path="$1" && shift
    strip "$archive_path/bin/ytserver" --strip-debug
    strip "$archive_path/node_modules/yt/lib/ytnode.node" --strip-debug
    strip "$archive_path/python/yt_driver_bindings/driver_lib.so" --strip-debug
    strip "$archive_path/python/yt_yson_bindings/yson_lib.so" --strip-debug
}

upload_to_sandbox() {
    local yt_version="$1" && shift
    local yt_local_version="$1" && shift
    local yt_python_version="$1" && shift
    local yt_yson_bindings_version="$1" && shift

    local archive_path="$1" && shift

    local task=$(cat <<EOF
{
    "type": "REMOTE_COPY_RESOURCE",
    "context":
    {
        "resource_type": "YT_LOCAL",
        "remote_file_protocol": "http",
        "remote_file_name": "http://locke.yt.yandex.net/api/v2/download?path=${archive_path}&disposition=attachment",
        "created_resource_name": "yt.tar",
        "resource_attrs": "ttl=inf,\
                           backup_task=true,\
                           yt_version=${yt_version},\
                           yt_local_version=${yt_local_version},\
                           yt_python_version=${yt_python_version},\
                           yt_yson_bindings_version=${yt_yson_bindings_version},\
                           yt_platform=${UBUNTU_CODENAME}"
    }
}
EOF
)
    local task_id=$(echo -ne "$task" | \
                    sandbox_request "POST" "task" -d @- | \
                    python -c 'import sys, json; print json.load(sys.stdin)["id"]')

    echo "Created sandbox task: $task_id"

    local task_params=$(cat <<EOF
{
    "description": "Upload YT local archive",
    "notifications": [
        {
            "transport": "email",
            "recipients": ["asaitgalin", "alexeyche", "vartyukh"],
            "statuses": ["SUCCESS"]
        },
        {
            "transport": "email",
            "recipients": ["asaitgalin"],
            "statuses": ["FAILURE", "TIMEOUT", "EXCEPTION"]
        }
    ],
    "owner": "TEAMCITY"
}
EOF
)

    echo -ne "$task_params" | sandbox_request "PUT" "task/${task_id}" -d @-

    echo -ne "[$task_id]" | sandbox_request "PUT" "batch/tasks/start" -d @-

    echo "Successfully started sandbox task $task_id"
}

YANDEX_YT_LOCAL_VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
YANDEX_YT_PYTHON_VERSION="0.6.89-0"
YANDEX_YT_VERSIONS="0.17.5-prestable-without-yt~7966~df46c24 18.2.19636-prestable-without-yt~ba0b505"
YANDEX_YT_YSON_BINDINGS_VERSION="0.2.26-0"

create_and_upload_archive() {
    local yt_local_version="$1" && shift
    local yt_version="$1" && shift
    local yt_python_version="$1" && shift
    local yt_yson_bindings_version="$1" && shift

    local versions_str="${yt_local_version}${yt_python_version}${yt_version}${yt_yson_bindings_version}"
    local hash_str=$(echo -ne $versions_str | md5sum | head -c 8)
    local archive_name="yt_local_${hash_str}_${UBUNTU_CODENAME}_archive.tar"
    local archive_path="//home/files/${archive_name}"

    echo -ne "Making archive with the following packages:\n" \
             "    yandex-yt=${yt_version}\n" \
             "    yandex-yt-local=${yt_local_version}\n" \
             "    yandex-yt-python=${yt_python_version}\n" \
             "    yandex-yt-python-yson=${yt_yson_bindings_version}\n"

    if [ "$($YT exists ${archive_path})" = "true" ]; then
        echo "Appropriate archive already exists"
        return
    fi

    # Download and unpack all necessary packages.
    "$(dirname "$0")/prepare_archive_directory.sh" "$yt_version" \
                                                   "$yt_python_version" \
                                                   "$yt_yson_bindings_version"
    local archive_local_path="$(cat yt_local_archive_path)"

    # Remove debug symbols from libraries and binaries.
    strip_debug_info "$archive_local_path"

    # Pack directory to tar archive without compression.
    local archive_local_name="$(mktemp /tmp/${archive_name}.XXXXXX)"
    tar cvf "$archive_local_name" -C "$archive_local_path" .

    cat "$archive_local_name" | $YT write-file "$archive_path"
    $YT set //home/files/${archive_name}/@packages_versions "{\
          yandex-yt=\"$yt_version\"; \
          yandex-yt-local=\"$yt_local_version\";\
          yandex-yt-python=\"$yt_python_version\";\
          yandex-yt-python-yson=\"$yt_yson_bindings_version\"}"

    upload_to_sandbox "$yt_version" \
                      "$yt_local_version" \
                      "$yt_python_version" \
                      "$yt_yson_bindings_version" \
                      "$archive_path"

    rm -rf "$archive_local_path"
    rm -rf "$archive_local_name"
    rm -rf "yt_local_archive_path"

    echo "Done! Archive path: $archive_path"
}

for YANDEX_YT_VERSION in $YANDEX_YT_VERSIONS; do
    create_and_upload_archive "$YANDEX_YT_LOCAL_VERSION" \
                              "$YANDEX_YT_VERSION" \
                              "$YANDEX_YT_PYTHON_VERSION" \
                              "$YANDEX_YT_YSON_BINDINGS_VERSION"
done
