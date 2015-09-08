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

    std::vector<Stroka> GetClusterNames() const;

    void RemoveCluster(const Stroka& clusterName);

    void UpdateCluster(const Stroka& clusterName, NApi::TConnectionConfigPtr config);

    void UpdateSelf();

private:
    const NApi::IConnectionPtr SelfConnection_;
    const NApi::IClientPtr SelfClient_;

    struct TCluster
    {
        Stroka Name;
        NApi::TConnectionConfigPtr Config;
        NApi::IConnectionPtr Connection;
    };

    TSpinLock Lock_;
    yhash_map<NObjectClient::TCellTag, TCluster> CellTagToCluster_;
    yhash_map<Stroka, TCluster> NameToCluster_;


    TCluster CreateCluster(const Stroka& name, NApi::TConnectionConfigPtr config) const;
    TCluster CreateSelfCluster() const;

    static NObjectClient::TCellTag GetCellTag(const TCluster& cluster);

};

DEFINE_REFCOUNTED_TYPE(TClusterDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

