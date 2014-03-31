#pragma once

#include <core/rpc/public.h>

#include <ytlib/object_client/public.h>
#include <ytlib/meta_state/public.h>

namespace NYT {
namespace NCellDirectory {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectory
    : public virtual TRefCounted
{
public:
    explicit TCellDirectory(NRpc::IChannelPtr masterChannel);

    NRpc::IChannelPtr GetChannel(NObjectClient::TCellId cellId) const;
    NRpc::IChannelPtr GetChannelOrThrow(NObjectClient::TCellId cellId) const;

    NRpc::IChannelPtr GetChannel(const Stroka& clusterName) const;
    NRpc::IChannelPtr GetChannelOrThrow(const Stroka& clusterName) const;

    NMetaState::TMasterDiscoveryConfigPtr GetMasterConfig(const Stroka& clusterName) const;

    std::vector<Stroka> GetClusterNames() const;

    void Remove(const Stroka& clusterName);

    void Update(const Stroka& clusterName, NMetaState::TMasterDiscoveryConfigPtr masterConfig);

    void UpdateSelf();

private:
    struct TCluster
    {
        NRpc::IChannelPtr Channel;
        NMetaState::TMasterDiscoveryConfigPtr MasterConfig;
        NObjectClient::TCellId CellId;
        Stroka Name;
    };

    yhash_map<NObjectClient::TCellId, TCluster> CellIdMap_;
    yhash_map<Stroka, TCluster> NameMap_;

    NRpc::IChannelPtr MasterChannel_;

    TCluster CreateCluster(
        const Stroka& name,
        NRpc::IChannelPtr channel,
        NMetaState::TMasterDiscoveryConfigPtr masterConfig) const;

    TSpinLock Lock_;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NCellDirectory
} // namespace NYT

