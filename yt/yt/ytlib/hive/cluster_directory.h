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

    //! Returns the connection to cluster with a given #clusterTag.
    //! Only applies to native connections. Returns |nullptr| if no connection is found.
    NApi::NNative::IConnectionPtr FindConnection(NApi::TClusterTag clusterTag) const;
    //! Same as #FindConnection but throws if no connection is found.
    NApi::NNative::IConnectionPtr GetConnectionOrThrow(NApi::TClusterTag clusterTag) const;

    //! Returns the connection to cluster with a given #clusterName.
    //! Returns |nullptr| if no connection is found.
    NApi::NNative::IConnectionPtr FindConnection(const TString& clusterName) const;
    //! Same as #FindConnection but throws if no connection is found.
    NApi::NNative::IConnectionPtr GetConnectionOrThrow(const TString& clusterName) const;

    //! Returns the list of names of all registered clusters.
    std::vector<TString> GetClusterNames() const;

    //! Removes the cluster of a given #name.
    //! Does nothing if no such cluster is registered.
    void RemoveCluster(const TString& name);

    //! Drops all directory entries.
    void Clear();

    //! Updates the configuration of a cluster with a given #name, recreates the connection.
    void UpdateCluster(const TString& name, NYTree::INodePtr nativeConnectionConfig);

    //! Updates configuration of all clusters given in #protoDirectory.
    //! Removes all clusters that are currently known but are missing in #protoDirectory.
    void UpdateDirectory(const NProto::TClusterDirectory& protoDirectory);

    //! Returns true if there is a cluster with corresponding TVM id in the directory.
    bool HasTvmId(NAuth::TTvmId tvmId) const;

    DEFINE_SIGNAL(void(const TString&, NYTree::INodePtr), OnClusterUpdated);

private:
    struct TCluster
    {
        TString Name;
        NYTree::INodePtr NativeConnectionConfig;
        NApi::NNative::IConnectionPtr Connection;
    };

    const NApi::NNative::TConnectionOptions ConnectionOptions_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<NApi::TClusterTag, TCluster> ClusterTagToCluster_;
    THashMap<TString, TCluster> NameToCluster_;
    THashMultiSet<NAuth::TTvmId> ClusterTvmIds_;

    TCluster CreateCluster(const TString& name, NYTree::INodePtr nativeConnectionConfig);
    static NApi::TClusterTag GetClusterTag(const TCluster& cluster);
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
    NApi::NNative::IClientPtr FindClient(const TString& clusterName) const;
    //! Same as #FindClient but throws if no connection is found in the
    //! underlying cluster directory.
    NApi::NNative::IClientPtr GetClientOrThrow(const TString& clusterName) const;

private:
    TClusterDirectoryPtr ClusterDirectory_;
    NApi::TClientOptions ClientOptions_;
};

DEFINE_REFCOUNTED_TYPE(TClientDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient

