#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>
#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/client_common.h>

#include <yt/yt/client/federated/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

TErrorOr<EQueueFamily> DeduceQueueFamily(
    const NQueueClient::TQueueTableRow& row,
    const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& replicatedTableMappingRow);

bool IsReplicatedTableObjectType(NCypressClient::EObjectType type);
bool IsReplicatedTableObjectType(const std::optional<NCypressClient::EObjectType>& type);

////////////////////////////////////////////////////////////////////////////////

//! A queue agent-specific client directory wrapper that supports working with replicated objects.
class TQueueAgentClientDirectory
    : public TRefCounted
{
public:
    explicit TQueueAgentClientDirectory(NHiveClient::TClientDirectoryPtr clientDirectory);

    struct TClientContext
    {
        NApi::IClientPtr Client;
        NYPath::TYPath Path;
    };

    struct TNativeClientContext
    {
        NApi::NNative::IClientPtr Client;
        NYPath::TYPath Path;
    };

    //! Returns client context suitable for reading the contents of the object referenced by the snapshot.
    //! For regular objects and replicated tables this corresponds to their control cluster and path.
    //! For chaos replicated tables we return a federated client wrapped over replica-clusters for chaos replicated tables.
    //! If `onlyDataReplicas` is true, only data replicas are considered for chaos replicated tables.
    template <class TObjectSnapshotPtr>
    TClientContext GetDataReadContext(const TObjectSnapshotPtr& snapshot, bool onlyDataReplicas = false);

    //! Returns native client context suitable for retrieving metadata for the object referenced by the snapshot.
    //! For regular objects this corresponds to their cluster and path.
    //! For replicated tables and chaos replicated tables we return the context for one of their sync replicas.
    //! If `onlyDataReplicas` is true, only data replicas are considered for chaos replicated tables.
    template <class TObjectSnapshotPtr>
    TNativeClientContext GetNativeSyncClient(const TObjectSnapshotPtr& snapshot, bool onlyDataReplicas = false);

    //! Proxy-method for internal client directory.
    NApi::NNative::IClientPtr GetClientOrThrow(const TString& cluster) const;

    //! Returns a federated client wrapped over the provided replica-clusters.
    //! NB: Each path must contain a cluster.
    NApi::IClientPtr GetFederatedClient(const std::vector<NYPath::TRichYPath>& replicas);

    NHiveClient::TClientDirectoryPtr GetUnderlyingClientDirectory() const;

private:
    const NHiveClient::TClientDirectoryPtr ClientDirectory_;
    const NClient::NFederated::TFederationConfigPtr FederationConfig_;

    THashMap<TString, NApi::IClientPtr> FederatedClients_;

    NApi::NNative::IConnectionPtr GetNativeConnection(const TString& cluster) const;
};

DEFINE_REFCOUNTED_TYPE(TQueueAgentClientDirectory)

//! Extracts replicas from replicated table mapping row with additional filters.
//! Returns all replicas by default.
//! If `onlySyncReplicas` is set, only replicas with mode=sync are returned.
//! If `onlyDataReplicas` is set, only replicas with content_type=data are returned. Applicable only for chaos.
//! If `validatePaths` is set, this function will validate that all replicas share the same path and are all located on
//! different clusters. Otherwise, an exception is thrown.
std::vector<NYPath::TRichYPath> GetRelevantReplicas(
    const NQueueClient::TReplicatedTableMappingTableRow& row,
    bool onlySyncReplicas = false,
    bool onlyDataReplicas = false,
    bool validatePaths = false);

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IClientPtr AssertNativeClient(const NApi::IClientPtr& client);

////////////////////////////////////////////////////////////////////////////////

//! Collect cumulative row indices from rows with given (tablet_index, row_index) pairs and
//! return them as a tablet_index: (row_index: cumulative_data_weight) map.
THashMap<int, THashMap<i64, i64>> CollectCumulativeDataWeights(
    const NYPath::TYPath& path,
    const NApi::IClientPtr& client,
    const std::vector<std::pair<int, i64>>& tabletAndRowIndices,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

//! Returns null if any of the arguments is null and their difference otherwise.
std::optional<i64> OptionalSub(std::optional<i64> lhs, std::optional<i64> rhs);

//! Returns the minimal non-null argument.
template <class T>
std::optional<T> MinOrValue(std::optional<T> lhs, std::optional<T> rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
