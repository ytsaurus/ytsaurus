#pragma once

#include <core/rpc/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/api/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectory
    : public virtual TRefCounted
{
public:
    explicit TClusterDirectory(NApi::IConnectionPtr selfConnection);

    NApi::IConnectionPtr GetConnection(NObjectClient::TCellTag cellTag) const;
    NApi::IConnectionPtr GetConnectionOrThrow(NObjectClient::TCellTag cellTag) const;

    NApi::IConnectionPtr GetConnection(const Stroka& clusterName) const;
    NApi::IConnectionPtr GetConnectionOrThrow(const Stroka& clusterName) const;

    TNullable<Stroka> GetDefaultNetwork(const Stroka& clusterName) const;

    NApi::TConnectionConfigPtr GetConnectionConfig(const Stroka& clusterName) const;
    NApi::TConnectionConfigPtr GetConnectionConfigOrThrow(const Stroka& clusterName) const;

    std::vector<Stroka> GetClusterNames() const;

    void RemoveCluster(const Stroka& clusterName);

    void UpdateCluster(
        const Stroka& clusterName,
        NApi::TConnectionConfigPtr config,
        NObjectClient::TCellTag cellTag,
        const TNullable<Stroka>& defaultNetwork);

    void UpdateSelf();

private:
    NApi::IConnectionPtr SelfConnection_;
    NApi::IClientPtr SelfClient_;

    struct TCluster
    {
        NApi::IConnectionPtr Connection;
        NApi::TConnectionConfigPtr ConnectionConfig;
        NObjectClient::TCellTag CellTag;
        Stroka Name;
        TNullable<Stroka> DefaultNetwork;
    };

    TSpinLock Lock_;
    yhash_map<NObjectClient::TCellTag, TCluster> CellTagToCluster_;
    yhash_map<Stroka, TCluster> NameToCluster_;


    TCluster CreateCluster(
        const Stroka& name,
        NApi::TConnectionConfigPtr config,
        NObjectClient::TCellTag cellTag,
        TNullable<Stroka> defaultNetwork) const;
    TCluster CreateSelfCluster() const;

};

DEFINE_REFCOUNTED_TYPE(TClusterDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

