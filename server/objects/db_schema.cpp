#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TSchemasTable SchemasTable;
const TObjectTableBase ObjectsTable;
const TParentsTable ParentsTable;
const TTombstonesTable TombstonesTable;
const TNodesTable NodesTable;
const TResourcesTable ResourcesTable;
const TPodsTable PodsTable;
const TPodSetsTable PodSetsTable;
const TNodeToPodsTable NodeToPodsTable;
const TAnnotationsTable AnnotationsTable;
const TNetworkProjectsTable NetworkProjectsTable;
const TDynamicResourcesTable DynamicResourcesTable;
const TReplicaSetsTable ReplicaSetsTable;
const TDaemonSetsTable DaemonSetsTable;
const TNodeSegmentToReplicaSetsTable NodeSegmentToReplicaSetsTable;
const TIP6NoncesTable IP6NoncesTable;
const TEndpointsTable EndpointsTable;
const TEndpointSetsTable EndpointSetsTable;
const TNodeSegmentsTable NodeSegmentsTable;
const TNodeSegmentToPodSetsTable NodeSegmentToPodSetsTable;
const TVirtualServicesTable VirtualServicesTable;
const TSubjectToTypeTable SubjectToTypeTable;
const TUsersTable UsersTable;
const TGroupsTable GroupsTable;
const TInternetAddressesTable InternetAddressesTable;
const TIP4AddressPoolsTable IP4AddressPoolsTable;
const TAccountsTable AccountsTable;
const TAccountParentToChildrenTable AccountParentToChildrenTable;
const TAccountToPodSetsTable AccountToPodSetsTable;
const TAccountToReplicaSetsTable AccountToReplicaSetsTable;
const TAccountToMultiClusterReplicaSetsTable AccountToMultiClusterReplicaSetsTable;
const TAccountToPodsTable AccountToPodsTable;
const TAccountToStagesTable AccountToStagesTable;
const TAccountToProjectsTable AccountToProjectsTable;
const TDnsRecordSetsTable DnsRecordSetsTable;
const TResourceCachesTable ResourceCachesTable;
const TMultiClusterReplicaSetsTable MultiClusterReplicaSetsTable;
const TNodeSegmentToMultiClusterReplicaSetsTable NodeSegmentToMultiClusterReplicaSetsTable;
const TStagesTable StagesTable;
const TPodDisruptionBudgetsTable PodDisruptionBudgetsTable;
const TPodDisruptionBudgetToPodSetsTable PodDisruptionBudgetToPodSetsTable;
const THistoryEventsTable HistoryEventsTable;
const TProjectsTable ProjectsTable;
const TReleaseRulesTable ReleaseRulesTable;
const TReleasesTable ReleasesTable;
const TDeployTicketsTable DeployTicketsTable;
const TReleaseToDeployTicketsTable ReleaseToDeployTicketsTable;
const TReleaseRuleToDeployTicketsTable ReleaseRuleToDeployTicketsTable;
const THorizontalPodAutoscalersTable HorizontalPodAutoscalersTable;
const TPersistentDisksTable PersistentDisksTable;
const TPersistentVolumesTable PersistentVolumesTable;
const TNodeToPersistentDisksTable NodeToPersistentDisksTable;
const TPersistentDiskToVolumesTable PersistentDiskToVolumesTable;
const TPersistentVolumeClaimsTable PersistentVolumeClaimsTable;
const TPodToMountedPersistentVolumesTable PodToMountedPersistentVolumesTable;

const TWatchLogSchema WatchLogSchema;

const std::vector<const TDBTable*> Tables = {
    &SchemasTable,
    &ParentsTable,
    &TombstonesTable,
    &NodesTable,
    &ResourcesTable,
    &PodsTable,
    &PodSetsTable,
    &NodeToPodsTable,
    &AnnotationsTable,
    &NetworkProjectsTable,
    &DynamicResourcesTable,
    &ReplicaSetsTable,
    &DaemonSetsTable,
    &NodeSegmentToReplicaSetsTable,
    &IP6NoncesTable,
    &EndpointsTable,
    &EndpointSetsTable,
    &NodeSegmentsTable,
    &NodeSegmentToPodSetsTable,
    &VirtualServicesTable,
    &SubjectToTypeTable,
    &UsersTable,
    &GroupsTable,
    &InternetAddressesTable,
    &IP4AddressPoolsTable,
    &AccountsTable,
    &AccountParentToChildrenTable,
    &AccountToPodSetsTable,
    &AccountToPodsTable,
    &AccountToReplicaSetsTable,
    &AccountToMultiClusterReplicaSetsTable,
    &AccountToStagesTable,
    &AccountToProjectsTable,
    &DnsRecordSetsTable,
    &ResourceCachesTable,
    &MultiClusterReplicaSetsTable,
    &NodeSegmentToMultiClusterReplicaSetsTable,
    &StagesTable,
    &PodDisruptionBudgetsTable,
    &PodDisruptionBudgetToPodSetsTable,
    &HistoryEventsTable,
    &ProjectsTable,
    &ReleaseRulesTable,
    &ReleasesTable,
    &DeployTicketsTable,
    &ReleaseToDeployTicketsTable,
    &ReleaseRuleToDeployTicketsTable,
    &HorizontalPodAutoscalersTable,
    &PersistentDisksTable,
    &PersistentVolumesTable,
    &NodeToPersistentDisksTable,
    &PersistentDiskToVolumesTable,
    &PersistentVolumeClaimsTable,
    &PodToMountedPersistentVolumesTable,
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
