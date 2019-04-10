#!/bin/bash -ex
# Upload archive with all necessary stuff to Locke and Sandbox.

set -o pipefail

export YT_PROXY=locke.yt.yandex.net

# Tokens belong to teamcity@ user
export YT_TOKEN=${TEAMCITY_YT_TOKEN}
# robot-yt-openstack token
export SANDBOX_TOKEN=${TEAMCITY_SANDBOX_TOKEN}

export PYTHONPATH="$(pwd)"

YT="$(pwd)/yt/wrapper/bin/yt"

UBUNTU_CODENAME=$(lsb_release -c -s)

sandbox_request() {
    local method="$1" && shift
    local path="$1" && shift

    touch _curl_out
    http_code=$(curl -X "$method" -sS -k -L -o "_curl_out" -w '%{http_code}' \
                "https://sandbox.yandex-team.ru/api/v1.0/${path}" \
                -H "Content-Type: application/json" \
                -H "Authorization: OAuth $SANDBOX_TOKEN" \
                "$@")

    if [[ $(($http_code / 100)) -ne "2" ]]; then
        echo "Sandbox request failed with error: $(cat _curl_out)"
        exit 1
    fi

    cat _curl_out
    rm -f "_curl_out"
}

strip_debug_info() {
    local archive_path="$1" && shift
    for binary in $(find "$archive_path/bin" -name "ytserver*"); do
        strip "$binary" --strip-debug
    done
    strip "$archive_path/python/yt_driver_bindings/driver_lib.so" --strip-debug
    strip "$archive_path/python/yt_yson_bindings/yson_lib.so" --strip-debug
}

upload_to_sandbox() {
    local yt_version="$1" && shift
    local yt_local_version="$1" && shift
    local yt_python_version="$1" && shift
    local yt_yson_bindings_version="$1" && shift

    local archive_path="$1" && shift

    local task;
    local task_id;
    local task_params;

    task=$(cat <<EOF
{
    "type": "REMOTE_COPY_RESOURCE",
    "context":
    {
        "resource_type": "YT_LOCAL",
        "remote_file_protocol": "http",
        "remote_file_name": "http://locke.yt.yandex.net/api/v3/read_file?path=${archive_path}&disposition=attachment",
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
    task_id=$(echo -ne "$task" | \
              sandbox_request "POST" "task" -d @- | \
              python2 -c 'import sys, json; print json.load(sys.stdin)["id"]')

    echo "Created sandbox task: $task_id"

    task_params=$(cat <<EOF
{
    "description": "Upload YT local archive",
    "notifications": [
        {
            "transport": "email",
            "recipients": ["asaitgalin", "alexeyche", "vartyukh", "ignat"],
            "statuses": ["SUCCESS"]
        },
        {
            "transport": "email",
            "recipients": ["asaitgalin", "ignat"],
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
YANDEX_YT_PYTHON_VERSION="0.8.49-0"

if [ "$UBUNTU_CODENAME" = "precise" ]; then
    YANDEX_YT_VERSIONS="19.4.28743-prestable-ya~a2b81d409e"
elif [ "$UBUNTU_CODENAME" = "trusty" ]; then
    YANDEX_YT_VERSIONS="19.3.27158-stable~a0b972103b"
else
    echo "Ubuntu $UBUNTU_CODENAME is not currently supported"
    exit 1
fi

YANDEX_YT_YSON_BINDINGS_VERSION="0.3.22-1"

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

    if [ "$($YT exists "${archive_path}/@success")" = "true" ]; then
        echo "Appropriate archive already exists"
        return
    fi

    # Download and unpack all necessary packages.
    "$(dirname "$0")/prepare_archive_directory.sh" "$yt_version" \
                                                   "$yt_python_version" \
                                                   "$yt_yson_bindings_version"
    local archive_local_path="$(cat yt_local_archive_path)"
    rm -rf "yt_local_archive_path"

    # Remove debug symbols from libraries and binaries.
    strip_debug_info "$archive_local_path"

    # Pack directory to tar archive without compression.
    local archive_local_name="$(mktemp /tmp/${archive_name}.XXXXXX)"
    tar cvf "$archive_local_name" -C "$archive_local_path" .
    rm -rf "$archive_local_path"

    cat "$archive_local_name" | $YT write-file "$archive_path"
    $YT set //home/files/${archive_name}/@packages_versions "{\
          yandex-yt=\"$yt_version\"; \
          yandex-yt-local=\"$yt_local_version\";\
          yandex-yt-python=\"$yt_python_version\";\
          yandex-yt-python-yson=\"$yt_yson_bindings_version\"}"

    $YT link "$archive_path" "//home/files/yt_local_archive.tar" --force

    rm -rf "$archive_local_name"

    upload_to_sandbox "$yt_version" \
                      "$yt_local_version" \
                      "$yt_python_version" \
                      "$yt_yson_bindings_version" \
                      "$archive_path"

    $YT set "${archive_path}/@success" "%true"

    echo "Done! Archive path: $archive_path"
}

for YANDEX_YT_VERSION in $YANDEX_YT_VERSIONS; do
    create_and_upload_archive "$YANDEX_YT_LOCAL_VERSION" \
                              "$YANDEX_YT_VERSION" \
                              "$YANDEX_YT_PYTHON_VERSION" \
                              "$YANDEX_YT_YSON_BINDINGS_VERSION"
done
