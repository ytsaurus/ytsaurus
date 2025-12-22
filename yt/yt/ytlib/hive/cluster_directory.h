#pragma once

#include "public.h"

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/options.h>

#include <yt/yt/client/hive/cluster_directory.h>

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
    : public TClusterDirectoryBase<NApi::NNative::IConnection>
{
    using TBase = TClusterDirectoryBase<NApi::NNative::IConnection>;

public:
    using TBase::TBase;

    NApi::NNative::IConnectionPtr CreateConnection(
        const std::string& name,
        const NYTree::INodePtr& connectionConfig) override;
    NObjectClient::TCellTagList GetCellTags(const TCluster& cluster) override;
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
    NApi::NNative::TClientOptions ClientOptions_;
};

DEFINE_REFCOUNTED_TYPE(TClientDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
