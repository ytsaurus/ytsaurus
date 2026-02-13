#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NCrossClusterReplicatedState {

////////////////////////////////////////////////////////////////////////////////

class ISingleClusterClient
    : public TRefCounted
{
public:
    template <std::movable T>
    TFuture<T> ExecuteCallback(
        std::string tag,
        TCallback<TFuture<T>(const ISingleClusterClientPtr&)> callback);

    virtual const NApi::IClientBasePtr& GetClient() = 0;
    virtual int GetIndex() const = 0;

protected:
    virtual TFuture<std::any> DoExecuteCallback(
        std::string tag,
        TCallback<TFuture<std::any>(const ISingleClusterClientPtr&)> callback) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISingleClusterClient);

////////////////////////////////////////////////////////////////////////////////

class IMultiClusterClient
    : public TRefCounted
{
public:
    template <std::movable T>
    TFuture<std::vector<TErrorOr<T>>> ExecuteCallback(
        std::string tag,
        TCallback<TFuture<T>(const ISingleClusterClientPtr&)> callback);

protected:
    virtual TFuture<std::vector<TErrorOr<std::any>>> DoExecuteCallback(
        std::string tag,
        TCallback<TFuture<std::any>(const ISingleClusterClientPtr&)> callback) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiClusterClient);

IMultiClusterClientPtr CreateCrossClusterClient(
    const NApi::NNative::IConnectionPtr& connection,
    const TCrossClusterReplicatedStateConfigPtr& config,
    const NApi::NNative::TClientOptions& options);

////////////////////////////////////////////////////////////////////////////////

std::vector<NApi::NNative::IConnectionPtr> CreateClusterConnections(
    const NApi::NNative::IConnectionPtr& connection,
    const TCrossClusterReplicatedStateConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState

#define CROSS_CLUSTER_CLIENT_INL_H_
#include "cross_cluster_client-inl.h"
#undef CROSS_CLUSTER_CLIENT_INL_H_
