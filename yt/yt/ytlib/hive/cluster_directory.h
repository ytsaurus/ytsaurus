#pragma once

#include "public.h"

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/hive/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

//! Maintains a map for a bunch of native cluster connections.
/*!
 *  Thread affinity: any
 */
class TClusterDirectory
    : public virtual TRefCounted
{
public:
    explicit TClusterDirectory(NApi::NNative::TConnectionOptions connectionOptions);

    //! Returns the connection to cluster with a given #cellTag.
    //! Only applies to native connections. Returns |nullptr| if no connection is found.
    NApi::NNative::IConnectionPtr FindConnection(NObjectClient::TCellTag cellTag) const;
    //! Same as #FindConnection but throws if no connection is found.
    NApi::NNative::IConnectionPtr GetConnectionOrThrow(NObjectClient::TCellTag cellTag) const;
    //! Same as #FindConnection but crashes if no connection is found.
    NApi::NNative::IConnectionPtr GetConnection(NObjectClient::TCellTag cellTag) const;

    //! Returns the connection to cluster with a given #clusterName.
    //! Returns |nullptr| if no connection is found.
    NApi::NNative::IConnectionPtr FindConnection(const std::string& clusterName) const;
    //! Same as #FindConnection but throws if no connection is found.
    NApi::NNative::IConnectionPtr GetConnectionOrThrow(const std::string& clusterName) const;
    //! Same as #FindConnection but crashes if no connection is found.
    NApi::NNative::IConnectionPtr GetConnection(const std::string& clusterName) const;

    //! Returns the list of names of all registered clusters.
    std::vector<std::string> GetClusterNames() const;

    //! Removes the cluster of a given #name.
    //! Does nothing if no such cluster is registered.
    void RemoveCluster(const std::string& name);

    //! Drops all directory entries.
    void Clear();

    //! Updates the configuration of a cluster with a given #name,
    //! recreates the connection if configuration changes.
    void UpdateCluster(const std::string& name, const NYTree::INodePtr& nativeConnectionConfig);

    //! Updates configuration of all clusters given in #protoDirectory.
    //! Removes all clusters that are currently known but are missing in #protoDirectory.
    void UpdateDirectory(const NProto::TClusterDirectory& protoDirectory);

    //! Returns true if there is a cluster with corresponding TVM id in the directory.
    bool HasTvmId(NAuth::TTvmId tvmId) const;

    DEFINE_SIGNAL(void(const std::string&, NYTree::INodePtr), OnClusterUpdated);

private:
    struct TCluster
    {
        std::string Name;
        NYTree::INodePtr NativeConnectionConfig;
        NApi::NNative::IConnectionPtr Connection;
    };

    const NApi::NNative::TConnectionOptions ConnectionOptions_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<NApi::TClusterTag, TCluster> CellTagToCluster_;
    THashMap<std::string, TCluster> NameToCluster_;
    THashMultiSet<NAuth::TTvmId> ClusterTvmIds_;

    TCluster CreateCluster(const std::string& name, const NYTree::INodePtr& nativeConnectionConfig);
    static NObjectClient::TCellTagList GetCellTags(const TCluster& cluster);
};

DEFINE_REFCOUNTED_TYPE(TClusterDirectory)

////////////////////////////////////////////////////////////////////////////////

//! A simple wrapper around #TClusterDirectory for creating clients
//! with given credentials.
class TClientDirectory
    : public TRefCounted
{
public:
    TClientDirectory(
        TClusterDirectoryPtr clusterDirectory,
        NApi::TClientOptions clientOptions);

    //! Returns the client to the cluster with a given #clusterName.
    //! Returns |nullptr| if no connection is found in the underlying cluster
    //! directory.
    NApi::NNative::IClientPtr FindClient(const std::string& clusterName) const;
    //! Same as #FindClient but throws if no connection is found in the
    //! underlying cluster directory.
    NApi::NNative::IClientPtr GetClientOrThrow(const std::string& clusterName) const;

private:
    TClusterDirectoryPtr ClusterDirectory_;
    NApi::TClientOptions ClientOptions_;
};

DEFINE_REFCOUNTED_TYPE(TClientDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
