#pragma once

#include "public.h"

#include "state_access.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TUnparsedYdbPayload
    : public NYTree::TYsonStruct
{
    std::optional<std::string> Data;

    REGISTER_YSON_STRUCT(TUnparsedYdbPayload);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUnparsedYdbPayload);

NYson::TYsonString SerializeUnparsedYdbPayload(const std::string& data);

////////////////////////////////////////////////////////////////////////////////

struct TYdbClusterInfo
    : public NYTree::TYsonStruct
{
    std::optional<i64> PartitionCount;
    std::optional<TInstant> ReadOnlyAt;

    REGISTER_YSON_STRUCT(TYdbClusterInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbClusterInfo);

struct TYdbTopicInfoControllerState
    : public NYTree::TYsonStruct
{
    THashMap<std::string, TYdbClusterInfoPtr> Clusters;
    THashMap<std::string, std::optional<i64>> CachedPartitionCounts;

    REGISTER_YSON_STRUCT(TYdbTopicInfoControllerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbTopicInfoControllerState);

TYdbTopologySnapshot ReadYdbStateSnapshot(const TYdbTopicInfoControllerState& state);
void WriteYdbStateSnapshot(const TYdbTopologySnapshot& snapshot, TYdbTopicInfoControllerState* state);

////////////////////////////////////////////////////////////////////////////////

struct TYdbPartitionNodeState
    : public NYTree::TYsonStruct
{
    i64 Id{};
    std::vector<i64> ParentIds;
    bool Active{};
    std::optional<i64> EndOffsetExclusive;
    bool Described{};
    bool Present{};
    bool Removed{};
    bool Scheduled{};
    bool InputExhausted{};
    bool Completed{};

    REGISTER_YSON_STRUCT(TYdbPartitionNodeState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbPartitionNodeState);

struct TYdbPartitionGraphClusterState
    : public NYTree::TYsonStruct
{
    bool TopologyKnown{};
    std::vector<TYdbPartitionNodeStatePtr> Partitions;

    REGISTER_YSON_STRUCT(TYdbPartitionGraphClusterState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbPartitionGraphClusterState);

struct TYdbPartitionGraphState
    : public NYTree::TYsonStruct
{
    THashMap<std::string, TYdbPartitionGraphClusterStatePtr> Clusters;

    REGISTER_YSON_STRUCT(TYdbPartitionGraphState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbPartitionGraphState);

TYdbPartitionGraphSnapshot ReadYdbStateSnapshot(const TYdbPartitionGraphState& state);
void WriteYdbStateSnapshot(const TYdbPartitionGraphSnapshot& snapshot, TYdbPartitionGraphState* state);

////////////////////////////////////////////////////////////////////////////////

struct TYdbHeartbeatState
    : public NYTree::TYsonStruct
{
    std::string ProducerIdBase;

    REGISTER_YSON_STRUCT(TYdbHeartbeatState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbHeartbeatState);

TYdbHeartbeatSnapshot ReadYdbStateSnapshot(const TYdbHeartbeatState& state);
void WriteYdbStateSnapshot(const TYdbHeartbeatSnapshot& snapshot, TYdbHeartbeatState* state);

////////////////////////////////////////////////////////////////////////////////

struct TYdbTwoPhaseCommitSinkState
    : public NYTree::TYsonStruct
{
    std::string ProducerId;
    TMessageId MaxPersistedMessageId;
    i64 MaxPersistedSeqNo{};

    REGISTER_YSON_STRUCT(TYdbTwoPhaseCommitSinkState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbTwoPhaseCommitSinkState);

TYdbSinkCheckpoint ReadYdbStateSnapshot(const TYdbTwoPhaseCommitSinkState& state);
void WriteYdbStateSnapshot(const TYdbSinkCheckpoint& snapshot, TYdbTwoPhaseCommitSinkState* state);

////////////////////////////////////////////////////////////////////////////////

struct TYdbPendingPublicationState
    : public NYTree::TYsonStruct
{
    std::optional<ui64> IntPublicationId;
    std::optional<std::string> Endpoint;
    std::optional<std::string> Database;

    REGISTER_YSON_STRUCT(TYdbPendingPublicationState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbPendingPublicationState);

struct TYdbDeferredPublicationState
    : public NYTree::TYsonStruct
{
    std::vector<TYdbPendingPublicationStatePtr> PendingPublications;

    REGISTER_YSON_STRUCT(TYdbDeferredPublicationState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbDeferredPublicationState);

TYdbPublicationSnapshot ReadYdbStateSnapshot(const TYdbDeferredPublicationState& state);
void WriteYdbStateSnapshot(const TYdbPublicationSnapshot& snapshot, TYdbDeferredPublicationState* state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
