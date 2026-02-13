#pragma once
#ifndef CROSS_CLUSTER_CLIENT_INL_H_
#error "Direct inclusion of this file is not allowed, include xcluster_client.h"
// For the sake of sane code completion.
#include "cross_cluster_client.h"
#endif

namespace NYT::NCrossClusterReplicatedState {

////////////////////////////////////////////////////////////////////////////////

template <std::movable T>
TFuture<T> ISingleClusterClient::ExecuteCallback(
    std::string tag,
    TCallback<TFuture<T>(const ISingleClusterClientPtr&)> callback)
{
    return DoExecuteCallback(
        std::move(tag),
        BIND([callback = std::move(callback)] (const ISingleClusterClientPtr& client) mutable {
            return std::invoke(std::move(callback), client)
                .AsUnique()
                .Apply(BIND([] (T&& value) {
                    return std::any(std::move(value));
                }));
        }))
        .AsUnique()
        .Apply(BIND([] (std::any&& executeResult) {
            return std::any_cast<T>(std::move(executeResult));
        }));
}

////////////////////////////////////////////////////////////////////////////////

template <std::movable T>
TFuture<std::vector<TErrorOr<T>>> IMultiClusterClient::ExecuteCallback(
    std::string tag,
    TCallback<TFuture<T>(const ISingleClusterClientPtr&)> callback)
{
    return DoExecuteCallback(
        std::move(tag),
        BIND([callback = std::move(callback)] (const ISingleClusterClientPtr& client) mutable {
            return std::invoke(std::move(callback), client)
                .AsUnique()
                .Apply(BIND([] (T&& value) {
                    return std::any(std::move(value));
                }));
        }))
        .AsUnique()
        .Apply(BIND([] (std::vector<TErrorOr<std::any>>&& executeResults) {
            std::vector<TErrorOr<T>> results;
            results.reserve(executeResults.size());

            for (auto& executeResult : executeResults) {
                if (!executeResult.IsOK()) {
                    results.emplace_back(std::move(static_cast<TError&>(executeResult)));
                    continue;
                }
                results.emplace_back(std::any_cast<T>(std::move(executeResult).Value()));
            }

            return results;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
