#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <yt/yt/core/actions/callback.h>

#include <util/generic/hash.h>

#include <type_traits>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TYdbClusterSnapshot
{
    //! What this cluster itself reported, null until it first describes the topic.
    std::optional<i64> PartitionCount;
    //! When discovery last reported the cluster as not fully available. Refreshed on every such answer
    //! and never cleared, so consumers must compare it against now rather than test for presence.
    std::optional<TInstant> ReadOnlyAt;
    //! The latest attempt of this controller to describe the topic on this cluster failed. Never persisted.
    bool DescriptionFailed = false;
};

struct TYdbTopologySnapshot
{
    THashMap<std::string, TYdbClusterSnapshot> Clusters;
    //! Superseded by #Clusters, but still read and written: a binary that predates #Clusters would
    //! otherwise lose every partition count on upgrade and on rollback.
    THashMap<std::string, std::optional<i64>> CachedPartitionCounts;
};

struct TYdbPartitionNode
{
    std::vector<i64> ParentIds;
    bool Active = true;
    std::optional<i64> EndOffsetExclusive;
    //! Listed by some successful description of the topic.
    bool Described = false;
    //! Listed by the latest successful description of the topic.
    bool Present = false;
    //! The topic confirmed that the partition does not exist.
    bool Removed = false;
    //! Listed as a source key, so Flow may own its state even while its reads are blocked.
    bool Scheduled = false;
    bool InputExhausted = false;
    //! Flow completed the source partition of this node.
    bool Completed = false;
};

struct TYdbPartitionGraph
{
    //! Whether #Partitions reflects a successful description rather than only a legacy partition count.
    bool TopologyKnown = false;
    std::map<i64, TYdbPartitionNode> Partitions;
};

struct TYdbPartitionGraphSnapshot
{
    THashMap<std::string, TYdbPartitionGraph> Clusters;
};

struct TYdbHeartbeatSnapshot
{
    std::string ProducerIdBase;
};

struct TYdbSinkCheckpoint
{
    std::string ProducerId;
    TMessageId MaxPersistedMessageId;
    i64 MaxPersistedSeqNo = 0;
};

struct TYdbPendingPublicationSnapshot
{
    std::optional<ui64> IntPublicationId;
    std::optional<std::string> Endpoint;
    std::optional<std::string> Database;
};

struct TYdbPublicationSnapshot
{
    std::vector<TYdbPendingPublicationSnapshot> PendingPublications;
};

//! Persistent state of a shared engine, read and written as a whole through a concrete serializer.
template <class T>
struct TYdbStateAccess
{
    TCallback<void(IInitContextPtr)> Init;
    TCallback<T()> Read;
    TCallback<void(const T&)> Write;
};

template <>
struct TYdbStateAccess<TYdbSinkCheckpoint>
{
    TCallback<void(IInitContextPtr)> Init;
    TCallback<TYdbSinkCheckpoint()> Read;
    TCallback<void(const TYdbSinkCheckpoint&)> Write;
    TCallback<bool(const TMessageId&)> IsPersisted;
};

////////////////////////////////////////////////////////////////////////////////

//! Owns the state client of a concrete serializer |TState|; |TSnapshot| crosses it through the
//! ReadYdbStateSnapshot and WriteYdbStateSnapshot overloads declared next to |TState|.
template <class TState, class TSnapshot>
class TYdbStateBridge
    : public TRefCounted
{
public:
    explicit TYdbStateBridge(std::string name);

    TYdbStateAccess<TSnapshot> MakeAccess();

private:
    const std::string Name_;
    TMutableStateClient<TState> State_;

    void Init(const IInitContextPtr& initContext);
    TSnapshot Read() const;
    void Write(const TSnapshot& snapshot);
};

template <class TState, class TSnapshot>
TYdbStateAccess<TSnapshot> CreateYdbStateAccess(std::string name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define STATE_ACCESS_INL_H_
#include "state_access-inl.h"
#undef STATE_ACCESS_INL_H_
