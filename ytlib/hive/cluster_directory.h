#pragma once

#include <yt/client/api/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/public.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

//! Maintains a map for a bunch of cluster connections.
/*!
 *  Thread affinity: any
 */
class TClusterDirectory
    : public virtual TRefCounted
{
public:
    //! Returns the connection to cluster with a given #cellTag.
    //! Only applies to native connections. Returns |nullptr| if no connection is found.
    NApi::IConnectionPtr FindConnection(NObjectClient::TCellTag cellTag) const;
    //! Same as #FindConnection but throws instead of failing.
    NApi::IConnectionPtr GetConnectionOrThrow(NObjectClient::TCellTag cellTag) const;

    //! Returns the connection to cluster with a given #clusterName.
    //! Returns |nullptr| if no connection is found.
    NApi::IConnectionPtr FindConnection(const TString& clusterName) const;
    //! Same as #FindConnection but throws instead of failing.
    NApi::IConnectionPtr GetConnectionOrThrow(const TString& clusterName) const;

    //! Returns the list of names of all registered clusters.
    std::vector<TString> GetClusterNames() const;

    //! Removes the cluster of a given #name.
    //! Does nothing if no such cluster is registered.
    void RemoveCluster(const TString& name);

    //! Drops all directory entries.
    void Clear();

    //! Updates the configuration of a cluster with a given #name, recreates the connection.
    void UpdateCluster(const TString& name, NYTree::INodePtr config);

    //! Updates configuration of all clusters given in #protoDirectory.
    //! Removes all clusters that are currently known but are missing in #protoDirectory.
    void UpdateDirectory(const NProto::TClusterDirectory& protoDirectory);

private:
    struct TCluster
    {
        NYTree::INodePtr Config;
        NApi::IConnectionPtr Connection;
    };

    TSpinLock Lock_;
    THashMap<NObjectClient::TCellTag, TCluster> CellTagToCluster_;
    THashMap<TString, TCluster> NameToCluster_;


    TCluster CreateCluster(const TString& name, NYTree::INodePtr config) const;
    static NObjectClient::TCellTag GetCellTag(const TCluster& cluster);

};

DEFINE_REFCOUNTED_TYPE(TClusterDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient

