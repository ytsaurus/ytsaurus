#!/bin/bash

GIT_WORK_DIR=$(git rev-parse --show-toplevel)

# TODO(verytable): remove the following lines when DTCC-1308 is resolved
(cd $GIT_WORK_DIR && find ./yt/yt_proto -type f -exec sed -i -e 's/a.yandex-team.ru/ytsaurus.tech/g' {} \;)
(cd $GIT_WORK_DIR && find ./yt/yt/core/rpc/unittests/lib/ -type f -exec sed -i -e 's/a.yandex-team.ru/ytsaurus.tech/g' {} \;)

(cd $GIT_WORK_DIR && protoc --go_opt=module=ytsaurus.tech --go_out=. -I ./yt $(find ./yt/yt_proto/yt/client/api/rpc_proxy -iname "*.proto"))
(cd $GIT_WORK_DIR && protoc --go_opt=module=ytsaurus.tech --go_out=. -I ./yt $(find ./yt/yt_proto/yt/core/rpc/proto -iname "*.proto"))
(cd $GIT_WORK_DIR && protoc --go_opt=module=ytsaurus.tech --go_out=. -I ./yt $(find ./yt/yt_proto/yt/core/tracing/proto -iname "*.proto"))
(cd $GIT_WORK_DIR && protoc --go_opt=module=ytsaurus.tech --go_out=. -I ./yt $(find ./yt/yt_proto/yt/core/yson/proto -iname "*.proto"))
(cd $GIT_WORK_DIR && protoc --go_opt=module=ytsaurus.tech --go_out=. -I ./yt $(find ./yt/yt_proto/yt/core/ytree/proto -iname "*.proto"))
(cd $GIT_WORK_DIR && protoc --go_opt=module=ytsaurus.tech --go_out=. -I ./yt $(find ./yt/yt/core/rpc/unittests/lib/my_service.proto -iname "*.proto"))
(cd $GIT_WORK_DIR && protoc --go_opt=module=ytsaurus.tech --go_out=. -I ./yt $(find ./yt/yt_proto/yt/client/chaos_client/proto -iname "*.proto"))
(cd $GIT_WORK_DIR && protoc --go_opt=module=ytsaurus.tech --go_out=. -I ./yt $(find ./yt/yt_proto/yt/client/chunk_client/proto -iname "*.proto"))
(cd $GIT_WORK_DIR && protoc --go_opt=module=ytsaurus.tech --go_out=. -I ./yt $(find ./yt/yt_proto/yt/client/hive/proto -iname "*.proto"))
