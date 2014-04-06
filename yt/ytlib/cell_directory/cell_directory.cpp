#include "cell_directory.h"

#include <core/rpc/channel.h>
#include <core/ytree/ypath_proxy.h>
#include <core/concurrency/fiber.h>
#include <ytlib/meta_state/config.h>
#include <ytlib/meta_state/master_channel.h>
#include <ytlib/object_client/object_service_proxy.h>

namespace NYT {
namespace NCellDirectory {

using namespace NRpc;
using namespace NMetaState;
using namespace NObjectClient;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

template<class T>
std::vector<typename T::key_type> Keys(const T& obj) {
    std::vector<typename T::key_type> result;
    for (const auto& it : obj) {
        result.push_back(it.first);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TCellDirectory::TCellDirectory(IChannelPtr masterChannel)
    : MasterChannel_(masterChannel)
{ }

IChannelPtr TCellDirectory::GetChannel(TCellId cellId) const
{
    TGuard<TSpinLock> guard(Lock_);

    auto it = CellIdMap_.find(cellId);
    if (it == CellIdMap_.end()) {
        return nullptr;
    }
    return it->second.Channel;
}

IChannelPtr TCellDirectory::GetChannelOrThrow(TCellId cellId) const
{
    auto channel = GetChannel(cellId);
    if (!channel) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with cell id %s", ~ToString(cellId));
    }
    return channel;
}

IChannelPtr TCellDirectory::GetChannel(const Stroka& clusterName) const
{
    TGuard<TSpinLock> guard(Lock_);

    auto it = NameMap_.find(clusterName);
    if (it == NameMap_.end()) {
        return nullptr;
    }
    return it->second.Channel;
}

IChannelPtr TCellDirectory::GetChannelOrThrow(const Stroka& clusterName) const
{
    auto channel = GetChannel(clusterName);
    if (!channel) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with name %s", ~clusterName.Quote());
    }
    return channel;
}

NMetaState::TMasterDiscoveryConfigPtr TCellDirectory::GetMasterConfig(const Stroka& clusterName) const
{
    TGuard<TSpinLock> guard(Lock_);

    auto it = NameMap_.find(clusterName);
    if (it == NameMap_.end()) {
        return nullptr;
    }
    return it->second.MasterConfig;
}

std::vector<Stroka> TCellDirectory::GetClusterNames() const
{
    TGuard<TSpinLock> guard(Lock_);

    return Keys(NameMap_);
}

void TCellDirectory::Remove(const Stroka& clusterName)
{
    TGuard<TSpinLock> guard(Lock_);

    TCellId cellId = NameMap_[clusterName].CellId;
    NameMap_.erase(clusterName);
    CellIdMap_.erase(cellId);
}

void TCellDirectory::Update(const Stroka& clusterName, NMetaState::TMasterDiscoveryConfigPtr masterConfig)
{
    auto addNewCluster = [&] (const TCluster& cluster) {
        if (CellIdMap_.find(cluster.CellId) != CellIdMap_.end()) {
            THROW_ERROR_EXCEPTION("Duplicated cell id %d", cluster.CellId);
        }
        CellIdMap_[cluster.CellId] = cluster;
        NameMap_[cluster.Name] = cluster;
    };

    auto it = NameMap_.find(clusterName);
    if (it == NameMap_.end()) {
        auto cluster = CreateCluster(clusterName, CreateLeaderChannel(masterConfig), masterConfig);

        TGuard<TSpinLock> guard(Lock_);

        addNewCluster(cluster);
    } else if (!AreNodesEqual(
            ConvertToNode(*(it->second.MasterConfig)),
            ConvertToNode(*masterConfig)))
    {
        auto cluster = CreateCluster(clusterName, CreateLeaderChannel(masterConfig), masterConfig);

        TGuard<TSpinLock> guard(Lock_);

        CellIdMap_.erase(it->second.CellId);
        NameMap_.erase(it);
        addNewCluster(cluster);
    }
}

void TCellDirectory::UpdateSelf()
{
    auto cluster = CreateCluster("", MasterChannel_, nullptr);
    {
        TGuard<TSpinLock> guard(Lock_);

        CellIdMap_[cluster.CellId] = cluster;
    }
}

TCellDirectory::TCluster TCellDirectory::CreateCluster(
    const Stroka& name,
    NRpc::IChannelPtr channel,
    NMetaState::TMasterDiscoveryConfigPtr masterConfig) const
{
    TCluster cluster;
    cluster.Name = name;
    cluster.Channel = channel;
    cluster.MasterConfig = masterConfig;

    // Get Cell Id
    auto batchReq = TObjectServiceProxy(cluster.Channel).ExecuteBatch();
    batchReq->AddRequest(TYPathProxy::Get("//sys/@cell_id"), "get_cell_id");
    auto batchRsp = WaitFor(batchReq->Invoke());
    if (batchRsp->IsOK()) {
        auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_cell_id");
        cluster.CellId = ConvertTo<TCellId>(TYsonString(rsp->value()));
    }

    return cluster;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellDirectory
} // namespace NYT

