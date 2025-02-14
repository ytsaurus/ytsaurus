#pragma once

#include "public.h"

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/client/api/dynamic_table_client.h>

namespace NYT::NApi::NNative {

extern const std::string UpstreamReplicaIdAttributeName;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IPickReplicaSession)

struct IPickReplicaSession
    : TRefCounted
{
    using TResult = std::variant<TSelectRowsResult, NYson::TYsonString>;
    using TExecuteCallback = std::function<TResult(
        const std::string& clusterName,
        const std::string& queryString,
        const TSelectRowsOptionsBase& baseOptions)>;

    virtual TResult Execute(const IConnectionPtr& connection, TExecuteCallback callback) = 0;

    virtual bool IsFallbackRequired() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IPickReplicaSession)

////////////////////////////////////////////////////////////////////////////////

IPickReplicaSessionPtr CreatePickReplicaSession(
    NQueryClient::NAst::TQuery* query,
    const IConnectionPtr& connection,
    const NTabletClient::ITableMountCachePtr& mountCache,
    const ITableReplicaSynchronicityCachePtr& cache,
    const TSelectRowsOptionsBase& options);

////////////////////////////////////////////////////////////////////////////////

std::vector<TFuture<NTabletClient::TTableMountInfoPtr>> GetQueryTableInfos(
    NQueryClient::NAst::TQuery* query,
    const NTabletClient::ITableMountCachePtr& cache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
