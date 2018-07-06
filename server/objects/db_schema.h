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
        TDBField Meta_Id{"meta.id"};
        TDBField Meta_CreationTime{"meta.creation_time"};
        TDBField Meta_RemovalTime{"meta.removal_time"};
        TDBField Meta_InheritAcl{"meta.inherit_acl"};
        TDBField Meta_Acl{"meta.acl"};
        TDBField Labels{"labels"};
    } Fields;
} ObjectsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TSchemasTable
    : public TDBTable
        , public TObjectTableBase
{
    TSchemasTable()
        : TDBTable("schemas")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
    } Fields;
} SchemasTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TParentsTable
    : public TDBTable
{
    TParentsTable()
        : TDBTable("parents")
    {
        Key = {&Fields.ObjectId, &Fields.ObjectType};
    }

    struct TFields
    {
        TDBField ObjectId{"object_id"};
        TDBField ObjectType{"object_type"};
        TDBField ParentId{"parent_id"};
    } Fields;
} ParentsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNodesTable
    : public TDBTable
    , public TObjectTableBase
{
    TNodesTable()
        : TDBTable("nodes")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec{"spec"};
        TDBField Status_AgentAddress{"status.agent_address"};
        TDBField Status_EpochId{"status.epoch_id"};
        TDBField Status_LastSeenTime{"status.last_seen_time"};
        TDBField Status_HeartbeatSequenceNumber{"status.heartbeat_sequence_number"};
        TDBField Status_Other{"status.other"};
    } Fields;
} NodesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TResourcesTable
    : public TDBTable
    , public TObjectTableBase
{
    TResourcesTable()
        : TDBTable("resources")
    {
        Key = {&Fields.Meta_NodeId, &TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Meta_NodeId{"meta.node_id"};
        TDBField Meta_Kind{"meta.kind"};
        TDBField Spec{"spec"};
        TDBField Status_ScheduledAllocations{"status.scheduled_allocations"};
        TDBField Status_ActualAllocations{"status.actual_allocations"};
    } Fields;
} ResourcesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TPodsTable
    : public TDBTable
    , public TObjectTableBase
{
    TPodsTable()
        : TDBTable("pods")
    {
        Key = {&Fields.Meta_PodSetId, &TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Meta_PodSetId{"meta.pod_set_id"};
        TDBField Status_GenerationNumber{"status.generation_number"};
        TDBField Status_AgentSpecTimestamp{"status.agent_spec_timestamp"};
        TDBField Status_Agent_State{"status.agent.state"};
        TDBField Status_Agent_IssPayload{"status.agent.iss_payload"};
        TDBField Status_Agent_PodAgentPayload{"status.agent.pod_agent_payload"};
        TDBField Status_Other{"status.other"};
        TDBField Spec_NodeId{"spec.node_id"};
        TDBField Spec_IssPayload{"spec.iss_payload"};
        TDBField Spec_PodAgentPayload{"spec.pod_agent_payload"};
        TDBField Spec_EnableScheduling{"spec.enable_scheduling"};
        TDBField Spec_UpdateTag{"spec.update_tag"};
        TDBField Spec_Other{"spec.other"};
    } Fields;
} PodsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TPodSetsTable
    : public TDBTable
    , public TObjectTableBase
{
    TPodSetsTable()
        : TDBTable("pod_sets")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec_AntiaffinityConstraints{"spec.antiaffinity_constraints"};
        TDBField Spec_NodeSegmentId{"spec.node_segment_id"};
    } Fields;
} PodSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNodeToPodsTable
    : public TDBTable
{
    TNodeToPodsTable()
        : TDBTable("node_to_pods")
    {
        Key = {&Fields.NodeId, &Fields.PodId};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField NodeId{"node_id"};
        TDBField PodId{"pod_id"};
    } Fields;
} NodeToPodsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TAnnotationsTable
    : public TDBTable
{
    TAnnotationsTable()
        : TDBTable("annotations")
    {
        Key = {&Fields.ObjectId, &Fields.ObjectType, &Fields.Name};
    }

    struct TFields
    {
        TDBField ObjectId{"object_id"};
        TDBField ObjectType{"object_type"};
        TDBField Name{"name"};
        TDBField Value{"value"};
    } Fields;
} AnnotationsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNetworkProjectsTable
    : public TDBTable
    , public TObjectTableBase
{
    TNetworkProjectsTable()
        : TDBTable("network_projects")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec_ProjectId{"spec.project_id"};
    } Fields;
} NetworkProjectsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TVirtualServicesTable
    : public TDBTable
    , public TObjectTableBase
{
    TVirtualServicesTable()
        : TDBTable("virtual_services")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec{"spec"};
    } Fields;
} VirtualServicesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TIP6NoncesTable
    : public TDBTable
{
    TIP6NoncesTable()
        : TDBTable("ip6_nonces")
    {
        Key = {&Fields.NodeId, &Fields.Nonce};
    }

    struct TFields
    {
        TDBField NodeId{"node_id"};
        TDBField Nonce{"nonce"};
        TDBField PodId{"pod_id"};
    } Fields;
} IP6NoncesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TEndpointSetsTable
    : public TDBTable
    , public TObjectTableBase
{
    TEndpointSetsTable()
        : TDBTable("endpoint_sets")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec{"spec"};
    } Fields;
} EndpointSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TEndpointsTable
    : public TDBTable
    , public TObjectTableBase
{
    TEndpointsTable()
        : TDBTable("endpoints")
    {
        Key = {&Fields.Meta_EndpointSetId, &TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Meta_EndpointSetId{"meta.endpoint_set_id"};
        TDBField Spec{"spec"};
    } Fields;
} EndpointsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNodeSegmentsTable
    : public TDBTable
    , public TObjectTableBase
{
    TNodeSegmentsTable()
        : TDBTable("node_segments")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec{"spec"};
    } Fields;
} NodeSegmentsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TNodeSegmentToPodSetsTable
    : public TDBTable
{
    TNodeSegmentToPodSetsTable()
        : TDBTable("node_segment_to_pod_sets")
    {
        Key = {&Fields.NodeSegmentId, &Fields.PodSetId};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField NodeSegmentId{"node_segment_id"};
        TDBField PodSetId{"pod_set_id"};
    } Fields;
} NodeSegmentToPodSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TSubjectToTypeTable
    : public TDBTable
{
    TSubjectToTypeTable()
        : TDBTable("subject_to_type")
    {
        Key = {&Fields.SubjectId};
    }

    struct TFields
    {
        TDBField SubjectId{"subject_id"};
        TDBField Type{"type"};
    } Fields;
} SubjectToTypeTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TUsersTable
    : public TDBTable
    , public TObjectTableBase
{
    TUsersTable()
        : TDBTable("users")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
    } Fields;
} UsersTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TGroupsTable
    : public TDBTable
    , public TObjectTableBase
{
    TGroupsTable()
        : TDBTable("groups")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec{"spec"};
    } Fields;
} GroupsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TInternetAddressesTable
    : public TDBTable
    , public TObjectTableBase
{
    TInternetAddressesTable()
        : TDBTable("internet_addresses")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec{"spec"};
        TDBField Status{"status"};
    } Fields;
} InternetAddressesTable;

////////////////////////////////////////////////////////////////////////////////

extern const std::vector<const TDBTable*> Tables;

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
