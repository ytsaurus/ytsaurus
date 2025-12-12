#include "cross_cluster_client.h"
#include "cross_cluster_client_detail.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NCrossClusterReplicatedState {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

class TSingleClusterClient
    : public ISingleClusterClient
{
public:
    TSingleClusterClient(int index, IClientBasePtr client)
        : Client_(std::move(client))
        , Index_(index)
    { }

    const NApi::IClientBasePtr& GetClient() override
    {
        return Client_;
    }

    int GetIndex() const override
    {
        return Index_;
    }

    TFuture<std::any> DoExecuteCallback(
        std::string,
        TCallback<TFuture<std::any>(const ISingleClusterClientPtr&)> callback) override
    {
        return std::invoke(std::move(callback), MakeStrong(this));
    }

private:
    IClientBasePtr Client_;
    int Index_;
};

////////////////////////////////////////////////////////////////////////////////

class TMultiClusterClient
    : public IMultiClusterClient
{
public:
    TMultiClusterClient(
        const NNative::IConnectionPtr& connection,
        const TCrossClusterReplicatedStateConfigPtr& config,
        const NNative::TClientOptions& options)
        : Connections_(CreateClusterConnections(connection, config))
        , Clients_(CreateClusterClients(Connections_, options))
    { }

protected:
    TFuture<std::vector<TErrorOr<std::any>>> DoExecuteCallback(
        std::string,
        TCallback<TFuture<std::any>(const ISingleClusterClientPtr&)> callback) override
    {
        std::vector<TFuture<std::any>> clusterFutures;
        clusterFutures.reserve(Clients_.size());

        for (const auto& [index, client] : SEnumerate(Clients_)) {
            if (!client) {
                clusterFutures.push_back(MakeFuture<std::any>(TError("No client for cluster %v", index)));
                continue;
            }
            clusterFutures.push_back(callback(New<TSingleClusterClient>(index, client)));
        }
        return AllSet(std::move(clusterFutures));
    }

private:
    std::vector<NNative::IConnectionPtr> Connections_;
    std::vector<IClientBasePtr> Clients_;
};

IMultiClusterClientPtr CreateCrossClusterClient(
    const NNative::IConnectionPtr& connection,
    const TCrossClusterReplicatedStateConfigPtr& config,
    const NNative::TClientOptions& options)
{
    return New<TMultiClusterClient>(connection, config, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
