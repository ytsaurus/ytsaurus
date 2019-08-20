#pragma once

#include "persistence.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

extern const struct TObjectTableBase
{
    TObjectTableBase()
    { }

    struct TFields
    {
        TDBField Meta_Id{"meta.id", NTableClient::EValueType::String};
        TDBField Meta_Etc{"meta.etc", NTableClient::EValueType::Any};
        TDBField Meta_CreationTime{"meta.creation_time", NTableClient::EValueType::Uint64};
        TDBField Meta_RemovalTime{"meta.removal_time", NTableClient::EValueType::Uint64};
        TDBField Meta_InheritAcl{"meta.inherit_acl", NTableClient::EValueType::Boolean};
        TDBField Meta_Acl{"meta.acl", NTableClient::EValueType::Any};
        TDBField Labels{"labels", NTableClient::EValueType::Any};
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
        TDBField ObjectId{"object_id", NTableClient::EValueType::String};
        TDBField ObjectType{"object_type", NTableClient::EValueType::Int64};
        TDBField ParentId{"parent_id", NTableClient::EValueType::String};
    } Fields;
} ParentsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TTombstonesTable
    : public TDBTable
{
    TTombstonesTable()
        : TDBTable("tombstones")
    {
        Key = {&Fields.ObjectId, &Fields.ObjectType};
    }

    struct TFields
    {
        TDBField ObjectId{"object_id", NTableClient::EValueType::String};
        TDBField ObjectType{"object_type", NTableClient::EValueType::Int64};
        TDBField RemovalTime{"removal_time", NTableClient::EValueType::Uint64};
    } Fields;
} TombstonesTable;

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
        TDBField Spec{"spec", NTableClient::EValueType::Any};
        // TODO(babenko): consider moving to status.etc
        TDBField Status_AgentAddress{"status.agent_address", NTableClient::EValueType::String};
        TDBField Status_EpochId{"status.epoch_id", NTableClient::EValueType::String};
        TDBField Status_LastSeenTime{"status.last_seen_time", NTableClient::EValueType::Uint64};
        TDBField Status_HeartbeatSequenceNumber{"status.heartbeat_sequence_number", NTableClient::EValueType::Uint64};
        TDBField Status_HostManager{"status.host_manager", NTableClient::EValueType::Any};
        TDBField Status_Etc{"status.etc", NTableClient::EValueType::Any};
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
        TDBField Meta_NodeId{"meta.node_id", NTableClient::EValueType::String};
        TDBField Meta_Kind{"meta.kind", NTableClient::EValueType::Int64};
        TDBField Spec{"spec", NTableClient::EValueType::Any};
        TDBField Status_ScheduledAllocations{"status.scheduled_allocations", NTableClient::EValueType::Any};
        TDBField Status_ActualAllocations{"status.actual_allocations", NTableClient::EValueType::Any};
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
        TDBField Meta_PodSetId{"meta.pod_set_id", NTableClient::EValueType::String};
        TDBField Status_GenerationNumber{"status.generation_number", NTableClient::EValueType::Uint64};
        TDBField Status_AgentSpecTimestamp{"status.agent_spec_timestamp", NTableClient::EValueType::Uint64};
        TDBField Status_Agent_State{"status.agent.state", NTableClient::EValueType::Int64};
        TDBField Status_Agent_IssPayload{"status.agent.iss_payload", NTableClient::EValueType::String};
        TDBField Status_Agent_PodAgentPayload{"status.agent.pod_agent_payload", NTableClient::EValueType::Any};
        TDBField Status_DynamicResources{"status.dynamic_resources", NTableClient::EValueType::Any};
        TDBField Status_Agent_Etc{"status.agent.etc", NTableClient::EValueType::Any};
        TDBField Status_Etc{"status.etc", NTableClient::EValueType::Any};
        TDBField Spec_NodeId{"spec.node_id", NTableClient::EValueType::String};
        TDBField Spec_IssPayload{"spec.iss_payload", NTableClient::EValueType::String};
        TDBField Spec_PodAgentPayload{"spec.pod_agent_payload", NTableClient::EValueType::Any};
        TDBField Spec_EnableScheduling{"spec.enable_scheduling", NTableClient::EValueType::Boolean};
        TDBField Spec_Secrets{"spec.secrets", NTableClient::EValueType::Any};
        TDBField Spec_UpdateTag{"spec.update_tag", NTableClient::EValueType::Boolean};
        TDBField Spec_Etc{"spec.etc", NTableClient::EValueType::Any};
        TDBField Spec_AccountId{"spec.account_id", NTableClient::EValueType::String};
        TDBField Spec_DynamicResources{"spec.dynamic_resources", NTableClient::EValueType::Any};
        TDBField Spec_ResourceCache{"spec.resource_cache", NTableClient::EValueType::Any};
        TDBField Spec_DynamicAttributes{"spec.dynamic_attributes", NTableClient::EValueType::Any};
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
        TDBField Spec_AntiaffinityConstraints{"spec.antiaffinity_constraints", NTableClient::EValueType::Any};
        TDBField Spec_NodeSegmentId{"spec.node_segment_id", NTableClient::EValueType::String};
        TDBField Spec_AccountId{"spec.account_id", NTableClient::EValueType::String};
        TDBField Spec_PodDisruptionBudgetId{"spec.pod_disruption_budget_id", NTableClient::EValueType::String};
        TDBField Spec_NodeFilter{"spec.node_filter", NTableClient::EValueType::String};
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
    {
        TDBField NodeId{"node_id", NTableClient::EValueType::String};
        TDBField PodId{"pod_id", NTableClient::EValueType::String};
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
        TDBField ObjectId{"object_id", NTableClient::EValueType::String};
        TDBField ObjectType{"object_type", NTableClient::EValueType::Int64};
        TDBField Name{"name", NTableClient::EValueType::String};
        TDBField Value{"value", NTableClient::EValueType::Any};
    } Fields;
} AnnotationsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TReplicaSetsTable
    : public TDBTable
    , public TObjectTableBase
{
    TReplicaSetsTable()
        : TDBTable("replica_sets")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec_AccountId{"spec.account_id", NTableClient::EValueType::String};
        TDBField Spec_Etc{"spec.etc", NTableClient::EValueType::Any};
        TDBField Status{"status", NTableClient::EValueType::Any};
    } Fields;
} ReplicaSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TResourceCachesTable
    : public TDBTable
    , public TObjectTableBase
{
    TResourceCachesTable()
        : TDBTable("resource_caches")
    {
        Key = {&Fields.Meta_PodSetId, &TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Meta_PodSetId{"meta.pod_set_id", NTableClient::EValueType::String};
        TDBField Spec{"spec", NTableClient::EValueType::Any};
        TDBField Status{"status", NTableClient::EValueType::Any};
    } Fields;
} ResourceCachesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TDynamicResourcesTable
    : public TDBTable
    , public TObjectTableBase
{
    TDynamicResourcesTable()
        : TDBTable("dynamic_resources")
    {
        Key = {&Fields.Meta_PodSetId, &TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Meta_PodSetId{"meta.pod_set_id", NTableClient::EValueType::String};
        TDBField Spec{"spec", NTableClient::EValueType::Any};
        TDBField Status{"status", NTableClient::EValueType::Any};
    } Fields;
} DynamicResourcesTable;

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
        TDBField Spec_ProjectId{"spec.project_id", NTableClient::EValueType::String};
    } Fields;
} NetworkProjectsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TDnsRecordSetsTable
    : public TDBTable
    , public TObjectTableBase
{
    TDnsRecordSetsTable()
        : TDBTable("dns_record_sets")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec{"spec", NTableClient::EValueType::Any};
        TDBField Status{"status", NTableClient::EValueType::Any};
    } Fields;
} DnsRecordSetsTable;

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
        TDBField Spec{"spec", NTableClient::EValueType::Any};
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
        TDBField NodeId{"node_id", NTableClient::EValueType::String};
        TDBField Nonce{"nonce", NTableClient::EValueType::Uint64};
        TDBField PodId{"pod_id", NTableClient::EValueType::String};
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
        TDBField Spec{"spec", NTableClient::EValueType::Any};
        TDBField Status_LastEndpointsUpdateTag{"status.last_endpoints_update_tag", NTableClient::EValueType::Boolean};
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
        TDBField Meta_EndpointSetId{"meta.endpoint_set_id", NTableClient::EValueType::String};
        TDBField Spec{"spec", NTableClient::EValueType::Any};
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
        TDBField Spec{"spec", NTableClient::EValueType::Any};
        TDBField Status{"status", NTableClient::EValueType::Any};
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
    {
        TDBField NodeSegmentId{"node_segment_id", NTableClient::EValueType::String};
        TDBField PodSetId{"pod_set_id", NTableClient::EValueType::String};
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
        TDBField SubjectId{"subject_id", NTableClient::EValueType::String};
        TDBField Type{"type", NTableClient::EValueType::Int64};
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
        TDBField Spec{"spec", NTableClient::EValueType::Any};
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
        TDBField Spec{"spec", NTableClient::EValueType::Any};
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
        Key = {&Fields.Meta_IP4AddressPoolId, &TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Meta_IP4AddressPoolId{"meta.ip4_address_pool_id", NTableClient::EValueType::String};
        TDBField Spec{"spec", NTableClient::EValueType::Any};
        TDBField Status{"status", NTableClient::EValueType::Any};
    } Fields;
} InternetAddressesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TIP4AddressPoolsTable
    : public TDBTable
    , public TObjectTableBase
{
    TIP4AddressPoolsTable()
        : TDBTable("ip4_address_pools")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec{"spec", NTableClient::EValueType::Any};
        TDBField Status{"status", NTableClient::EValueType::Any};
    } Fields;
} IP4AddressPoolsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TAccountsTable
    : public TDBTable
    , public TObjectTableBase
{
    TAccountsTable()
        : TDBTable("accounts")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec_ParentId{"spec.parent_id", NTableClient::EValueType::String};
        TDBField Spec_Etc{"spec.etc", NTableClient::EValueType::Any};
        TDBField Status{"status", NTableClient::EValueType::Any};
    } Fields;
} AccountsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TAccountParentToChildrenTable
    : public TDBTable
{
    TAccountParentToChildrenTable()
        : TDBTable("account_parent_to_children")
    {
        Key = {&Fields.ParentId, &Fields.ChildId};
    }

    struct TFields
    {
        TDBField ParentId{"parent_id", NTableClient::EValueType::String};
        TDBField ChildId{"child_id", NTableClient::EValueType::String};
    } Fields;
} AccountParentToChildrenTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TAccountToPodSetsTable
    : public TDBTable
{
    TAccountToPodSetsTable()
        : TDBTable("account_to_pod_sets")
    {
        Key = {&Fields.AccountId, &Fields.PodSetId};
    }

    struct TFields
    {
        TDBField AccountId{"account_id", NTableClient::EValueType::String};
        TDBField PodSetId{"pod_set_id", NTableClient::EValueType::String};
    } Fields;
} AccountToPodSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TAccountToReplicaSetsTable
    : public TDBTable
{
    TAccountToReplicaSetsTable()
        : TDBTable("account_to_replica_sets")
    {
        Key = {&Fields.AccountId, &Fields.ReplicaSetId};
    }

    struct TFields
    {
        TDBField AccountId{"account_id", NTableClient::EValueType::String};
        TDBField ReplicaSetId{"replica_set_id", NTableClient::EValueType::String};
    } Fields;
} AccountToReplicaSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TAccountToMultiClusterReplicaSetsTable
    : public TDBTable
{
    TAccountToMultiClusterReplicaSetsTable()
        : TDBTable("account_to_multi_cluster_replica_sets")
    {
        Key = {&Fields.AccountId, &Fields.ReplicaSetId};
    }

    struct TFields
    {
        TDBField AccountId{"account_id", NTableClient::EValueType::String};
        TDBField ReplicaSetId{"replica_set_id", NTableClient::EValueType::String};
    } Fields;
} AccountToMultiClusterReplicaSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TAccountToPodsTable
    : public TDBTable
{
    TAccountToPodsTable()
        : TDBTable("account_to_pods")
    {
        Key = {&Fields.AccountId, &Fields.PodSetId};
    }

    struct TFields
    {
        TDBField AccountId{"account_id", NTableClient::EValueType::String};
        TDBField PodSetId{"pod_id", NTableClient::EValueType::String};
    } Fields;
} AccountToPodsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TMultiClusterReplicaSetsTable
    : public TDBTable
    , public TObjectTableBase
{
    TMultiClusterReplicaSetsTable()
        : TDBTable("multi_cluster_replica_sets")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec_AccountId{"spec.account_id", NTableClient::EValueType::String};
        TDBField Spec_Etc{"spec.etc", NTableClient::EValueType::Any};
        TDBField Status{"status", NTableClient::EValueType::Any};
    } Fields;
} MultiClusterReplicaSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TStagesTable
    : public TDBTable
    , public TObjectTableBase
{
    TStagesTable()
        : TDBTable("stages")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec_AccountId{"spec.account_id", NTableClient::EValueType::String};
        TDBField Spec_Etc{"spec.etc", NTableClient::EValueType::Any};
        TDBField Status{"status", NTableClient::EValueType::Any};
    } Fields;
} StagesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TAccountToStagesTable
    : public TDBTable
{
    TAccountToStagesTable()
        : TDBTable("account_to_stages")
    {
        Key = {&Fields.AccountId, &Fields.StageId};
    }

    struct TFields
    {
        TDBField AccountId{"account_id", NTableClient::EValueType::String};
        TDBField StageId{"stage_id", NTableClient::EValueType::String};
    } Fields;
} AccountToStagesTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TPodDisruptionBudgetsTable
    : public TDBTable
    , public TObjectTableBase
{
    TPodDisruptionBudgetsTable()
        : TDBTable("pod_disruption_budgets")
    {
        Key = {&TObjectTableBase::Fields.Meta_Id};
    }

    struct TFields
        : public TObjectTableBase::TFields
    {
        TDBField Spec{"spec", NTableClient::EValueType::Any};
        TDBField Status{"status", NTableClient::EValueType::Any};
        TDBField StatusUpdateTag{"status_update_tag", NTableClient::EValueType::Boolean};
    } Fields;
} PodDisruptionBudgetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const struct TPodDisruptionBudgetToPodSetsTable
    : public TDBTable
{
    TPodDisruptionBudgetToPodSetsTable()
        : TDBTable("pod_disruption_budget_to_pod_sets")
    {
        Key = {&Fields.PodDisruptionBudgetId, &Fields.PodSetId};
    }

    struct TFields
    {
        TDBField PodDisruptionBudgetId{"pod_disruption_budget_id", NTableClient::EValueType::String};
        TDBField PodSetId{"pod_set_id", NTableClient::EValueType::String};
    } Fields;
} PodDisruptionBudgetToPodSetsTable;

////////////////////////////////////////////////////////////////////////////////

extern const std::vector<const TDBTable*> Tables;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
