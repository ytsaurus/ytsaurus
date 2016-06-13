#pragma once

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class TClusterDirectory
    : public virtual TRefCounted
{
public:
    explicit TClusterDirectory(NApi::INativeConnectionPtr selfConnection);

    NApi::INativeConnectionPtr GetConnection(NObjectClient::TCellTag cellTag) const;
    NApi::INativeConnectionPtr GetConnectionOrThrow(NObjectClient::TCellTag cellTag) const;

    NApi::INativeConnectionPtr GetConnection(const Stroka& clusterName) const;
    NApi::INativeConnectionPtr GetConnectionOrThrow(const Stroka& clusterName) const;

    std::vector<Stroka> GetClusterNames() const;

    void RemoveCluster(const Stroka& clusterName);

    void UpdateCluster(const Stroka& clusterName, NApi::TNativeConnectionConfigPtr config);

    void UpdateSelf();

private:
    const NApi::INativeConnectionPtr SelfConnection_;
    const NApi::INativeClientPtr SelfClient_;

    struct TCluster
    {
        Stroka Name;
        NApi::TNativeConnectionConfigPtr Config;
        NApi::INativeConnectionPtr Connection;
    };

    TSpinLock Lock_;
    yhash_map<NObjectClient::TCellTag, TCluster> CellTagToCluster_;
    yhash_map<Stroka, TCluster> NameToCluster_;


    TCluster CreateCluster(const Stroka& name, NApi::TNativeConnectionConfigPtr config) const;
    TCluster CreateSelfCluster() const;

    static NObjectClient::TCellTag GetCellTag(const TCluster& cluster);

};

DEFINE_REFCOUNTED_TYPE(TClusterDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT

