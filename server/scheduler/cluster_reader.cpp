#include "cluster_reader.h"

#include "private.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yp/server/objects/db_schema.h>
#include <yp/server/objects/transaction.h>
#include <yp/server/objects/transaction_manager.h>

#include <yp/server/objects/proto/autogen.pb.h>
#include <yp/server/objects/proto/objects.pb.h>

#include <yp/server/lib/cluster/account.h>
#include <yp/server/lib/cluster/cluster_reader.h>
#include <yp/server/lib/cluster/internet_address.h>
#include <yp/server/lib/cluster/ip4_address_pool.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/node_segment.h>
#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/pod_disruption_budget.h>
#include <yp/server/lib/cluster/pod_set.h>
#include <yp/server/lib/cluster/resource.h>

#include <yt/client/table_client/helpers.h>

#include <yt/client/api/rowset.h>

#include <yt/core/ytree/node.h>

namespace NYP::NServer::NScheduler {

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NTableClient;
using namespace NYT::NYTree;
using namespace NYT::NYson;

using namespace NCluster;
using namespace NMaster;

using NObjects::AccountsTable;
using NObjects::IP4AddressPoolsTable;
using NObjects::InternetAddressesTable;
using NObjects::NodeSegmentsTable;
using NObjects::NodesTable;
using NObjects::ObjectsTable;
using NObjects::PodDisruptionBudgetsTable;
using NObjects::PodSetsTable;
using NObjects::PodsTable;
using NObjects::ResourcesTable;

////////////////////////////////////////////////////////////////////////////////

class TClusterReader
    : public IClusterReader
{
public:
    explicit TClusterReader(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual NObjects::TTimestamp StartTransaction() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        Transaction_ = WaitFor(transactionManager->StartReadOnlyTransaction())
            .ValueOrThrow();
        return Transaction_->GetStartTimestamp();
    }

    virtual void ReadIP4AddressPools(
        TObjectConsumer<TIP4AddressPool> ip4AddressPoolConsumer) override
    {
        ReadImpl(std::move(ip4AddressPoolConsumer), "IP4 address pools");
    }

    virtual void ReadInternetAddresses(
        TObjectConsumer<TInternetAddress> internetAddressConsumer) override
    {
        ReadImpl(std::move(internetAddressConsumer), "internet addresses");
    }

    virtual void ReadNodes(
        TObjectConsumer<TNode> nodeConsumer) override
    {
        ReadImpl(std::move(nodeConsumer), "nodes");
    }

    virtual void ReadAccounts(
        TObjectConsumer<TAccount> accountConsumer) override
    {
        ReadImpl(std::move(accountConsumer), "accounts");
    }

    virtual void ReadNodeSegments(
        TObjectConsumer<TNodeSegment> nodeSegmentConsumer) override
    {
        ReadImpl(std::move(nodeSegmentConsumer), "node segments");
    }

    virtual void ReadPodDisruptionBudgets(
        TObjectConsumer<TPodDisruptionBudget> podDisruptionBudgetConsumer) override
    {
        ReadImpl(std::move(podDisruptionBudgetConsumer), "pod disruption budgets");
    }

    virtual void ReadPodSets(
        TObjectConsumer<TPodSet> podSetConsumer) override
    {
        ReadImpl(std::move(podSetConsumer), "pod sets");
    }

    virtual void ReadPods(
        TObjectConsumer<TPod> podConsumer) override
    {
        ReadImpl(std::move(podConsumer), "pods");
    }

    virtual void ReadResources(
        TObjectConsumer<TResource> resourceConsumer) override
    {
        ReadImpl(std::move(resourceConsumer), "resources");
    }

private:
    TBootstrap* const Bootstrap_;

    NObjects::TTransactionPtr Transaction_;


    NObjects::ISession* GetSession()
    {
        return Transaction_->GetSession();
    }

    void Reset()
    {
        Transaction_.Reset();
    }


    template <class T>
    void ReadImpl(TObjectConsumer<T> consumer, TStringBuf pluralObjectName)
    {
        auto* session = GetSession();

        try {
            session->ScheduleLoad(
                [&] (NObjects::ILoadContext* context) {
                    context->ScheduleSelect(
                        GetObjectQueryString<T>(),
                        [&] (const IUnversionedRowsetPtr& rowset) {
                            YT_LOG_INFO("Parsing %v",
                                pluralObjectName);
                            for (auto row : rowset->GetRows()) {
                                consumer(ParseObjectFromRow<T>(row));
                            }
                        });
                });

            YT_LOG_INFO("Querying %v",
                pluralObjectName);
            session->FlushLoads();
        } catch (...) {
            // Make sure transaction does not hold scheduled callbacks with dangling references.
            try {
                Reset();
            } catch (...) {
                YT_ABORT();
            }
            throw;
        }
    }


    template <class T>
    TString GetObjectQueryString();

    template <class T>
    std::unique_ptr<T> ParseObjectFromRow(TUnversionedRow row);
};

////////////////////////////////////////////////////////////////////////////////

// NB! Gcc does not allow to define GetObjectQueryString<> and ParseObjectFromRow<>
//     template specializations directly inside TClusterReader body.

template <>
TString TClusterReader::GetObjectQueryString<TIP4AddressPool>()
{
    const auto& ytConnector = Bootstrap_->GetYTConnector();
    return Format(
        "[%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
        IP4AddressPoolsTable.Fields.Meta_Id.Name,
        IP4AddressPoolsTable.Fields.Labels.Name,
        IP4AddressPoolsTable.Fields.Spec.Name,
        IP4AddressPoolsTable.Fields.Status.Name,
        ytConnector->GetTablePath(&IP4AddressPoolsTable),
        ObjectsTable.Fields.Meta_RemovalTime.Name);
}

template <>
std::unique_ptr<TIP4AddressPool> TClusterReader::ParseObjectFromRow<TIP4AddressPool>(
    TUnversionedRow row)
{
    TObjectId ip4AddressPoolId;
    TYsonString labels;
    NClient::NApi::NProto::TIP4AddressPoolSpec spec;
    NClient::NApi::NProto::TIP4AddressPoolStatus status;
    FromUnversionedRow(
        row,
        &ip4AddressPoolId,
        &labels,
        &spec,
        &status);

    return std::make_unique<TIP4AddressPool>(
        std::move(ip4AddressPoolId),
        std::move(labels),
        std::move(spec),
        std::move(status));
}

////////////////////////////////////////////////////////////////////////////////

template <>
TString TClusterReader::GetObjectQueryString<TInternetAddress>()
{
    const auto& ytConnector = Bootstrap_->GetYTConnector();
    return Format(
        "[%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
        InternetAddressesTable.Fields.Meta_IP4AddressPoolId.Name,
        InternetAddressesTable.Fields.Meta_Id.Name,
        InternetAddressesTable.Fields.Labels.Name,
        InternetAddressesTable.Fields.Spec.Name,
        InternetAddressesTable.Fields.Status.Name,
        ytConnector->GetTablePath(&InternetAddressesTable),
        ObjectsTable.Fields.Meta_RemovalTime.Name);
}

template <>
std::unique_ptr<TInternetAddress> TClusterReader::ParseObjectFromRow<TInternetAddress>(
    TUnversionedRow row)
{
    TObjectId ip4AddressPoolId;
    TObjectId internetAddressId;
    TYsonString labels;
    NClient::NApi::NProto::TInternetAddressSpec spec;
    NClient::NApi::NProto::TInternetAddressStatus status;
    FromUnversionedRow(
        row,
        &ip4AddressPoolId,
        &internetAddressId,
        &labels,
        &spec,
        &status);

    return std::make_unique<TInternetAddress>(
        std::move(internetAddressId),
        std::move(ip4AddressPoolId),
        std::move(labels),
        std::move(spec),
        std::move(status));
}

////////////////////////////////////////////////////////////////////////////////

template <>
TString TClusterReader::GetObjectQueryString<TNode>()
{
    const auto& ytConnector = Bootstrap_->GetYTConnector();
    return Format(
        "[%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
        NodesTable.Fields.Meta_Id.Name,
        NodesTable.Fields.Labels.Name,
        NodesTable.Fields.Status_Etc.Name,
        NodesTable.Fields.Spec.Name,
        ytConnector->GetTablePath(&NodesTable),
        ObjectsTable.Fields.Meta_RemovalTime.Name);
}

template <>
std::unique_ptr<TNode> TClusterReader::ParseObjectFromRow<TNode>(
    TUnversionedRow row)
{
    TObjectId nodeId;
    TYsonString labels;
    NObjects::NProto::TNodeStatusEtc statusEtc;
    NClient::NApi::NProto::TNodeSpec spec;
    FromUnversionedRow(
        row,
        &nodeId,
        &labels,
        &statusEtc,
        &spec);

    return std::make_unique<TNode>(
        std::move(nodeId),
        std::move(labels),
        static_cast<NObjects::EHfsmState>(statusEtc.hfsm().state()),
        static_cast<NObjects::ENodeMaintenanceState>(statusEtc.maintenance().state()),
        statusEtc.unknown_pod_ids_size(),
        std::move(spec));
}

////////////////////////////////////////////////////////////////////////////////

template <>
TString TClusterReader::GetObjectQueryString<TAccount>()
{
    const auto& ytConnector = Bootstrap_->GetYTConnector();
    return Format(
        "[%v], [%v], [%v] from [%v] where is_null([%v])",
        AccountsTable.Fields.Meta_Id.Name,
        AccountsTable.Fields.Labels.Name,
        AccountsTable.Fields.Spec_ParentId.Name,
        ytConnector->GetTablePath(&AccountsTable),
        AccountsTable.Fields.Meta_RemovalTime.Name);
}

template <>
std::unique_ptr<TAccount> TClusterReader::ParseObjectFromRow<TAccount>(
    TUnversionedRow row)
{
    TObjectId accountId;
    TYsonString labels;
    TObjectId parentId;
    FromUnversionedRow(
        row,
        &accountId,
        &labels,
        &parentId);

    return std::make_unique<TAccount>(
        std::move(accountId),
        std::move(labels),
        std::move(parentId));
}

////////////////////////////////////////////////////////////////////////////////

template <>
TString TClusterReader::GetObjectQueryString<TNodeSegment>()
{
    const auto& ytConnector = Bootstrap_->GetYTConnector();
    return Format(
        "[%v], [%v], [%v] from [%v] where is_null([%v])",
        NodeSegmentsTable.Fields.Meta_Id.Name,
        NodesTable.Fields.Labels.Name,
        NodeSegmentsTable.Fields.Spec.Name,
        ytConnector->GetTablePath(&NodeSegmentsTable),
        NodeSegmentsTable.Fields.Meta_RemovalTime.Name);
}

template <>
std::unique_ptr<TNodeSegment> TClusterReader::ParseObjectFromRow<TNodeSegment>(
    TUnversionedRow row)
{
    TObjectId segmentId;
    TYsonString labels;
    NClient::NApi::NProto::TNodeSegmentSpec spec;
    FromUnversionedRow(
        row,
        &segmentId,
        &labels,
        &spec);

    return std::make_unique<TNodeSegment>(
        std::move(segmentId),
        std::move(labels),
        std::move(*spec.mutable_node_filter()));
}

////////////////////////////////////////////////////////////////////////////////

template <>
TString TClusterReader::GetObjectQueryString<TPodDisruptionBudget>()
{
    const auto& ytConnector = Bootstrap_->GetYTConnector();
    return Format(
        "[%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
        PodDisruptionBudgetsTable.Fields.Meta_Id.Name,
        PodDisruptionBudgetsTable.Fields.Labels.Name,
        PodDisruptionBudgetsTable.Fields.Meta_Etc.Name,
        PodDisruptionBudgetsTable.Fields.Spec.Name,
        PodDisruptionBudgetsTable.Fields.Status.Name,
        ytConnector->GetTablePath(&PodDisruptionBudgetsTable),
        ObjectsTable.Fields.Meta_RemovalTime.Name);
}

template <>
std::unique_ptr<TPodDisruptionBudget> TClusterReader::ParseObjectFromRow<TPodDisruptionBudget>(
    TUnversionedRow row)
{
    TObjectId id;
    TYsonString labels;
    NServer::NObjects::NProto::TMetaEtc metaEtc;
    NClient::NApi::NProto::TPodDisruptionBudgetSpec spec;
    NClient::NApi::NProto::TPodDisruptionBudgetStatus status;
    FromUnversionedRow(
        row,
        &id,
        &labels,
        &metaEtc,
        &spec,
        &status);

    return std::make_unique<TPodDisruptionBudget>(
        std::move(id),
        std::move(labels),
        std::move(*metaEtc.mutable_uuid()),
        std::move(spec),
        std::move(status));
}

////////////////////////////////////////////////////////////////////////////////

template <>
TString TClusterReader::GetObjectQueryString<TPodSet>()
{
    const auto& ytConnector = Bootstrap_->GetYTConnector();
    return Format(
        "[%v], [%v], [%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
        PodSetsTable.Fields.Meta_Id.Name,
        PodSetsTable.Fields.Labels.Name,
        PodSetsTable.Fields.Spec_AntiaffinityConstraints.Name,
        PodSetsTable.Fields.Spec_NodeSegmentId.Name,
        PodSetsTable.Fields.Spec_AccountId.Name,
        PodSetsTable.Fields.Spec_NodeFilter.Name,
        PodSetsTable.Fields.Spec_PodDisruptionBudgetId.Name,
        ytConnector->GetTablePath(&PodSetsTable),
        ObjectsTable.Fields.Meta_RemovalTime.Name);
}

template <>
std::unique_ptr<TPodSet> TClusterReader::ParseObjectFromRow<TPodSet>(
    TUnversionedRow row)
{
    TObjectId podSetId;
    TYsonString labels;
    std::vector<NClient::NApi::NProto::TAntiaffinityConstraint> antiaffinityConstraints;
    TObjectId nodeSegmentId;
    TObjectId accountId;
    TString nodeFilter;
    TObjectId podDisruptionBudgetId;
    FromUnversionedRow(
        row,
        &podSetId,
        &labels,
        &antiaffinityConstraints,
        &nodeSegmentId,
        &accountId,
        &nodeFilter,
        &podDisruptionBudgetId);

    return std::make_unique<TPodSet>(
        std::move(podSetId),
        std::move(labels),
        std::move(nodeSegmentId),
        std::move(accountId),
        std::move(podDisruptionBudgetId),
        std::move(antiaffinityConstraints),
        std::move(nodeFilter));
}

////////////////////////////////////////////////////////////////////////////////

template <>
TString TClusterReader::GetObjectQueryString<TPod>()
{
    const auto& ytConnector = Bootstrap_->GetYTConnector();
    return Format(
        "[%v], [%v], [%v], [%v], [%v], [%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
        PodsTable.Fields.Meta_Id.Name,
        PodsTable.Fields.Meta_PodSetId.Name,
        PodsTable.Fields.Meta_Etc.Name,
        PodsTable.Fields.Spec_NodeId.Name,
        PodsTable.Fields.Spec_Etc.Name,
        PodsTable.Fields.Spec_AccountId.Name,
        PodsTable.Fields.Spec_EnableScheduling.Name,
        PodsTable.Fields.Status_Etc.Name,
        PodsTable.Fields.Status_Scheduling_Etc.Name,
        PodsTable.Fields.Labels.Name,
        ytConnector->GetTablePath(&PodsTable),
        ObjectsTable.Fields.Meta_RemovalTime.Name);
}

template <>
std::unique_ptr<TPod> TClusterReader::ParseObjectFromRow<TPod>(
    TUnversionedRow row)
{
    TObjectId podId;
    TObjectId podSetId;
    NServer::NObjects::NProto::TMetaEtc metaEtc;
    TObjectId nodeId;
    NServer::NObjects::NProto::TPodSpecEtc specEtc;
    TObjectId accountId;
    bool enableScheduling;
    NServer::NObjects::NProto::TPodStatusEtc statusEtc;
    NServer::NObjects::NProto::TPodStatusSchedulingEtc statusSchedulingEtc;
    TYsonString labels;
    FromUnversionedRow(
        row,
        &podId,
        &podSetId,
        &metaEtc,
        &nodeId,
        &specEtc,
        &accountId,
        &enableScheduling,
        &statusEtc,
        &statusSchedulingEtc,
        &labels);

    return std::make_unique<TPod>(
        std::move(podId),
        std::move(labels),
        std::move(podSetId),
        std::move(nodeId),
        std::move(accountId),
        std::move(*metaEtc.mutable_uuid()),
        std::move(*specEtc.mutable_resource_requests()),
        // NB! Pass some arguments by const reference due to lack of move semantics support.
        specEtc.disk_volume_requests(),
        specEtc.gpu_requests(),
        specEtc.ip6_address_requests(),
        specEtc.ip6_subnet_requests(),
        std::move(*specEtc.mutable_node_filter()),
        enableScheduling,
        std::move(*statusEtc.mutable_eviction()),
        std::move(*statusSchedulingEtc.mutable_error()));
}

////////////////////////////////////////////////////////////////////////////////

template <>
TString TClusterReader::GetObjectQueryString<TResource>()
{
    const auto& ytConnector = Bootstrap_->GetYTConnector();
    return Format(
        "[%v], [%v], [%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
        ResourcesTable.Fields.Meta_Id.Name,
        ResourcesTable.Fields.Meta_NodeId.Name,
        ResourcesTable.Fields.Meta_Kind.Name,
        ResourcesTable.Fields.Spec.Name,
        ResourcesTable.Fields.Status_ScheduledAllocations.Name,
        ResourcesTable.Fields.Status_ActualAllocations.Name,
        ResourcesTable.Fields.Labels.Name,
        ytConnector->GetTablePath(&ResourcesTable),
        ObjectsTable.Fields.Meta_RemovalTime.Name);
}

template <>
std::unique_ptr<TResource> TClusterReader::ParseObjectFromRow<TResource>(
    TUnversionedRow row)
{
    TObjectId resourceId;
    TObjectId nodeId;
    EResourceKind kind;
    TYsonString labels;
    NClient::NApi::NProto::TResourceSpec spec;
    std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation> scheduledAllocations;
    std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation> actualAllocations;
    FromUnversionedRow(
        row,
        &resourceId,
        &nodeId,
        &kind,
        &spec,
        &scheduledAllocations,
        &actualAllocations,
        &labels);

    return std::make_unique<TResource>(
        std::move(resourceId),
        std::move(labels),
        std::move(nodeId),
        kind,
        std::move(spec),
        std::move(scheduledAllocations),
        std::move(actualAllocations));
}

////////////////////////////////////////////////////////////////////////////////

IClusterReaderPtr CreateClusterReader(TBootstrap* bootstrap)
{
    return New<TClusterReader>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
