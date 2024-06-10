#pragma once

#include <contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>

#include <contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/common/type_switcher.h>

#include <contrib/ydb/public/sdk/cpp/client/ydb_types/status_codes.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_types/ydb.h>

#include <contrib/ydb/library/yql/public/issue/yql_issue.h>

#include <contrib/ydb/library/grpc/client/grpc_client_low.h>


namespace NYdb {

// Other callbacks
using TSimpleCb = std::function<void()>;
using TErrorCb = std::function<void(NYdbGrpc::TGrpcStatus&)>;

struct TBalancingSettings {
    EBalancingPolicy Policy;
    TStringType PolicyParams;
};

} // namespace NYdb
