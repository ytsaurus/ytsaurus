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
const TReplicaSetsTable ReplicaSetsTable;
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
const TAccountsTable AccountsTable;
const TAccountParentToChildrenTable AccountParentToChildrenTable;
const TAccountToPodSetsTable AccountToPodSetsTable;
const TAccountToReplicaSetsTable AccountToReplicaSetsTable;
const TAccountToPodsTable AccountToPodsTable;
const TDnsRecordSetsTable DnsRecordSetsTable;
const TResourceCachesTable ResourceCachesTable;

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
    &ReplicaSetsTable,
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
    &AccountsTable,
    &AccountParentToChildrenTable,
    &AccountToPodSetsTable,
    &AccountToPodsTable,
    &DnsRecordSetsTable,
    &ResourceCachesTable
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

