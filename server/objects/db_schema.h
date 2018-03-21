#pragma once

#include "persistence.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

extern const struct TObjectTableBase
{
    TObjectTableBase()
    { }

    struct TFields
    {
        TDbField Meta_Id{"meta.id"};
        TDbField Meta_CreationTime{"meta.creation_time"};
        TDbField Meta_RemovalTime{"meta.removal_time"};
        TDbField Labels{"labels"};
    } Fields;
} ObjectsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TParentsTable
    : public TDbTable
{
    TParentsTable()
        : TDbTable("parents")
    {
        Key = {&Fields.ObjectId, &Fields.ObjectType};
    }

    struct TFields
    {
        TDbField ObjectId{"object_id"};
        TDbField ObjectType{"object_type"};
        TDbField ParentId{"parent_id"};
    } Fields;
} ParentsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNodesTable
    : public TDbTable
    , public TObjectTableBase
{
    TNodesTable()
        : TDbTable("nodes")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDbField Spec{"spec"};
        TDbField Status_AgentAddress{"status.agent_address"};
        TDbField Status_EpochId{"status.epoch_id"};
        TDbField Status_LastSeenTime{"status.last_seen_time"};
        TDbField Status_HeartbeatSequenceNumber{"status.heartbeat_sequence_number"};
        TDbField Status_Other{"status.other"};
    } Fields;
} NodesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TResourcesTable
    : public TDbTable
    , public TObjectTableBase
{
    TResourcesTable()
        : TDbTable("resources")
    {
        Key = {&Fields.Meta_NodeId, &TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDbField Meta_NodeId{"meta.node_id"};
        TDbField Spec{"spec"};
        TDbField Status_ScheduledAllocations{"status.scheduled_allocations"};
        TDbField Status_ActualAllocations{"status.actual_allocations"};
    } Fields;
} ResourcesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TPodsTable
    : public TDbTable
    , public TObjectTableBase
{
    TPodsTable()
        : TDbTable("pods")
    {
        Key = {&Fields.Meta_PodSetId, &TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDbField Meta_PodSetId{"meta.pod_set_id"};
        TDbField Status_GenerationNumber{"status.generation_number"};
        TDbField Status_AgentSpecTimestamp{"status.agent_spec_timestamp"};
        TDbField Status_Agent_State{"status.agent.state"};
        TDbField Status_Agent_IssPayload{"status.agent.iss_payload"};
        TDbField Status_Other{"status.other"};
        TDbField Spec_NodeId{"spec.node_id"};
        TDbField Spec_IssPayload{"spec.iss_payload"};
        TDbField Spec_EnableScheduling{"spec.enable_scheduling"};
        TDbField Spec_UpdateTag{"spec.update_tag"};
        TDbField Spec_Other{"spec.other"};
    } Fields;
} PodsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TPodSetsTable
    : public TDbTable
    , public TObjectTableBase
{
    TPodSetsTable()
        : TDbTable("pod_sets")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDbField Spec_AntiaffinityConstraints{"spec.antiaffinity_constraints"};
        TDbField Spec_NodeSegmentId{"spec.node_segment_id"};
    } Fields;
} PodSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNodeToPodsTable
    : public TDbTable
{
    TNodeToPodsTable()
        : TDbTable("node_to_pods")
    {
        Key = {&Fields.NodeId, &Fields.PodId};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDbField NodeId{"node_id"};
        TDbField PodId{"pod_id"};
    } Fields;
} NodeToPodsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TAnnotationsTable
    : public TDbTable
{
    TAnnotationsTable()
        : TDbTable("annotations")
    {
        Key = {&Fields.ObjectId, &Fields.ObjectType, &Fields.Name};
    }

    struct TFields
    {
        TDbField ObjectId{"object_id"};
        TDbField ObjectType{"object_type"};
        TDbField Name{"name"};
        TDbField Value{"value"};
    } Fields;
} AnnotationsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNetworkProjectsTable
    : public TDbTable
    , public TObjectTableBase
{
    TNetworkProjectsTable()
        : TDbTable("network_projects")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDbField Spec_ProjectId{"spec.project_id"};
    } Fields;
} NetworkProjectsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TIP6NoncesTable
    : public TDbTable
{
    TIP6NoncesTable()
        : TDbTable("ip6_nonces")
    {
        Key = {&Fields.NodeId, &Fields.Nonce};
    }

    struct TFields
    {
        TDbField NodeId{"node_id"};
        TDbField Nonce{"nonce"};
        TDbField PodId{"pod_id"};
    } Fields;
} IP6NoncesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TEndpointSetsTable
    : public TDbTable
    , public TObjectTableBase
{
    TEndpointSetsTable()
        : TDbTable("endpoint_sets")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDbField Spec{"spec"};
    } Fields;
} EndpointSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TEndpointsTable
    : public TDbTable
    , public TObjectTableBase
{
    TEndpointsTable()
        : TDbTable("endpoints")
    {
        Key = {&Fields.Meta_EndpointSetId, &TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDbField Meta_EndpointSetId{"meta.endpoint_set_id"};
        TDbField Spec{"spec"};
    } Fields;
} EndpointsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNodeSegmentsTable
    : public TDbTable
    , public TObjectTableBase
{
    TNodeSegmentsTable()
        : TDbTable("node_segments")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDbField Spec{"spec"};
    } Fields;
} NodeSegmentsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNodeSegmentToPodSetsTable
    : public TDbTable
{
    TNodeSegmentToPodSetsTable()
        : TDbTable("node_segment_to_pod_sets")
    {
        Key = {&Fields.NodeSegmentId, &Fields.PodSetId};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDbField NodeSegmentId{"node_segment_id"};
        TDbField PodSetId{"pod_set_id"};
    } Fields;
} NodeSegmentToPodSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const std::vector<const TDbTable*> Tables;

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
