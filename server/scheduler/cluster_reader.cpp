#include "cluster_reader.h"

#include "account.h"
#include "internet_address.h"
#include "ip4_address_pool.h"
#include "node.h"
#include "node_segment.h"
#include "pod.h"
#include "pod_disruption_budget.h"
#include "pod_set.h"
#include "private.h"
#include "resource.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yp/server/objects/transaction_manager.h>
#include <yp/server/objects/transaction.h>
#include <yp/server/objects/db_schema.h>

#include <yp/server/objects/proto/autogen.pb.h>
#include <yp/server/objects/proto/objects.pb.h>

#include <yt/client/table_client/helpers.h>

#include <yt/client/api/rowset.h>

#include <yt/core/ytree/node.h>

namespace NYP::NServer::NScheduler {

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NTableClient;
using namespace NYT::NYTree;
using namespace NYT::NYson;

using namespace NMaster;
using namespace NObjects;

////////////////////////////////////////////////////////////////////////////////

class TClusterReader
    : public IClusterReader
{
public:
    explicit TClusterReader(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual TTimestamp StartTransaction() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        Transaction_ = WaitFor(transactionManager->StartReadOnlyTransaction())
            .ValueOrThrow();
        return Transaction_->GetStartTimestamp();
    }

    virtual void ReadIP4AddressPools(
        TObjectConsumer<TIP4AddressPool> ip4AddressPoolConsumer) override
    {
        auto* session = GetSession();

        session->ScheduleLoad(
            [&] (ILoadContext* context) {
                context->ScheduleSelect(
                    GetIP4AddressPoolQueryString(),
                    [&ip4AddressPoolConsumer, this] (const IUnversionedRowsetPtr& rowset) {
                        YT_LOG_INFO("Parsing IP4 address pools");
                        for (auto row : rowset->GetRows()) {
                            ip4AddressPoolConsumer(ParseIP4AddressPoolFromRow(row));
                        }
                    });
            });

        YT_LOG_INFO("Querying IP4 address pools");
        session->FlushLoads();
    }

    virtual void ReadInternetAddresses(
        TObjectConsumer<TInternetAddress> internetAddressConsumer) override
    {
        auto* session = GetSession();

        session->ScheduleLoad(
            [&] (ILoadContext* context) {
                context->ScheduleSelect(
                    GetInternetAddressQueryString(),
                    [&internetAddressConsumer, this] (const IUnversionedRowsetPtr& rowset) {
                        YT_LOG_INFO("Parsing internet addresses");
                        for (auto row : rowset->GetRows()) {
                            internetAddressConsumer(ParseInternetAddressFromRow(row));
                        }
                    });
            });

        YT_LOG_INFO("Querying internet addresses");
        session->FlushLoads();
    }

    virtual void ReadNodes(
        TObjectConsumer<TNode> nodeConsumer) override
    {
        auto* session = GetSession();

        session->ScheduleLoad(
            [&] (ILoadContext* context) {
                context->ScheduleSelect(
                    GetNodeQueryString(),
                    [&nodeConsumer, this] (const IUnversionedRowsetPtr& rowset) {
                        YT_LOG_INFO("Parsing nodes");
                        for (auto row : rowset->GetRows()) {
                            nodeConsumer(ParseNodeFromRow(row));
                        }
                    });
            });

        YT_LOG_INFO("Querying nodes");
        session->FlushLoads();
    }

    virtual void ReadAccounts(
        TObjectConsumer<TAccount> accountConsumer) override
    {
        auto* session = GetSession();

        session->ScheduleLoad(
            [&] (ILoadContext* context) {
                context->ScheduleSelect(
                    GetAccountQueryString(),
                    [&accountConsumer, this] (const IUnversionedRowsetPtr& rowset) {
                        YT_LOG_INFO("Parsing accounts");
                        for (auto row : rowset->GetRows()) {
                            accountConsumer(ParseAccountFromRow(row));
                        }
                    });
            });

        YT_LOG_INFO("Querying accounts");
        session->FlushLoads();
    }

    virtual void ReadNodeSegments(
        TObjectConsumer<TNodeSegment> nodeSegmentConsumer) override
    {
        auto* session = GetSession();

        session->ScheduleLoad(
            [&] (ILoadContext* context) {
                context->ScheduleSelect(
                    GetNodeSegmentQueryString(),
                    [&nodeSegmentConsumer, this] (const IUnversionedRowsetPtr& rowset) {
                        YT_LOG_INFO("Parsing node segments");
                        for (auto row : rowset->GetRows()) {
                            nodeSegmentConsumer(ParseNodeSegmentFromRow(row));
                        }
                    });
            });

        YT_LOG_INFO("Querying node segments");
        session->FlushLoads();
    }

    virtual void ReadPodDisruptionBudgets(
        TObjectConsumer<TPodDisruptionBudget> podDisruptionBudgetConsumer) override
    {
        auto* session = GetSession();

        session->ScheduleLoad(
            [&] (ILoadContext* context) {
                context->ScheduleSelect(
                    GetPodDisruptionBudgetQueryString(),
                    [&podDisruptionBudgetConsumer, this] (const IUnversionedRowsetPtr& rowset) {
                        YT_LOG_INFO("Parsing pod disruption budgets");
                        for (auto row : rowset->GetRows()) {
                            podDisruptionBudgetConsumer(ParsePodDisruptionBudgetFromRow(row));
                        }
                    });
            });

        YT_LOG_INFO("Querying pod disruption budgets");
        session->FlushLoads();
    }

    virtual void ReadPodSets(
        TObjectConsumer<TPodSet> podSetConsumer) override
    {
        auto* session = GetSession();

        session->ScheduleLoad(
            [&] (ILoadContext* context) {
                context->ScheduleSelect(
                    GetPodSetQueryString(),
                    [&podSetConsumer, this] (const IUnversionedRowsetPtr& rowset) {
                        YT_LOG_INFO("Parsing pod sets");
                        for (auto row : rowset->GetRows()) {
                            podSetConsumer(ParsePodSetFromRow(row));
                        }
                    });
            });

        YT_LOG_INFO("Querying pod sets");
        session->FlushLoads();
    }

    virtual void ReadPods(
        TObjectConsumer<TPod> podConsumer) override
    {
        auto* session = GetSession();

        session->ScheduleLoad(
            [&] (ILoadContext* context) {
                context->ScheduleSelect(
                    GetPodQueryString(),
                    [&podConsumer, this] (const IUnversionedRowsetPtr& rowset) {
                        YT_LOG_INFO("Parsing pods");
                        for (auto row : rowset->GetRows()) {
                            podConsumer(ParsePodFromRow(row));
                        }
                    });
            });

        YT_LOG_INFO("Querying pods");
        session->FlushLoads();
    }

    virtual void ReadResources(
        TObjectConsumer<TResource> resourceConsumer) override
    {
        auto* session = GetSession();

        session->ScheduleLoad(
            [&] (ILoadContext* context) {
                context->ScheduleSelect(
                    GetResourceQueryString(),
                    [&resourceConsumer, this] (const IUnversionedRowsetPtr& rowset) {
                        YT_LOG_INFO("Parsing resources");
                        for (auto row : rowset->GetRows()) {
                            resourceConsumer(ParseResourceFromRow(row));
                        }
                    });
            });

        YT_LOG_INFO("Querying resources");
        session->FlushLoads();
    }

private:
    TBootstrap* const Bootstrap_;

    TTransactionPtr Transaction_;


    ISession* GetSession()
    {
        return Transaction_->GetSession();
    }


    TString GetIP4AddressPoolQueryString()
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

    std::unique_ptr<TIP4AddressPool> ParseIP4AddressPoolFromRow(TUnversionedRow row)
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


    TString GetInternetAddressQueryString()
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

    std::unique_ptr<TInternetAddress> ParseInternetAddressFromRow(TUnversionedRow row)
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


    TString GetNodeQueryString()
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

    std::unique_ptr<TNode> ParseNodeFromRow(TUnversionedRow row)
    {
        TObjectId nodeId;
        TYsonString labels;
        NProto::TNodeStatusEtc statusEtc;
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
            static_cast<EHfsmState>(statusEtc.hfsm().state()),
            static_cast<ENodeMaintenanceState>(statusEtc.maintenance().state()),
            statusEtc.unknown_pod_ids_size(),
            std::move(spec));
    }


    TString GetAccountQueryString()
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

    std::unique_ptr<TAccount> ParseAccountFromRow(TUnversionedRow row)
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


    TString GetNodeSegmentQueryString()
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

    std::unique_ptr<TNodeSegment> ParseNodeSegmentFromRow(TUnversionedRow row)
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


    TString GetPodDisruptionBudgetQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
            PodDisruptionBudgetsTable.Fields.Meta_Id.Name,
            PodDisruptionBudgetsTable.Fields.Labels.Name,
            PodDisruptionBudgetsTable.Fields.Meta_Etc.Name,
            PodDisruptionBudgetsTable.Fields.Spec.Name,
            ytConnector->GetTablePath(&PodDisruptionBudgetsTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name);
    }

    std::unique_ptr<TPodDisruptionBudget> ParsePodDisruptionBudgetFromRow(TUnversionedRow row)
    {
        TObjectId id;
        TYsonString labels;
        NServer::NObjects::NProto::TMetaEtc metaEtc;
        NClient::NApi::NProto::TPodDisruptionBudgetSpec spec;
        FromUnversionedRow(
            row,
            &id,
            &labels,
            &metaEtc,
            &spec);

        return std::make_unique<TPodDisruptionBudget>(
            std::move(id),
            std::move(labels),
            std::move(*metaEtc.mutable_uuid()),
            std::move(spec));
    }

    TString GetPodSetQueryString()
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

    std::unique_ptr<TPodSet> ParsePodSetFromRow(TUnversionedRow row)
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


    TString GetPodQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v]) and [%v] = true",
            PodsTable.Fields.Meta_Id.Name,
            PodsTable.Fields.Meta_PodSetId.Name,
            PodsTable.Fields.Meta_Etc.Name,
            PodsTable.Fields.Spec_NodeId.Name,
            PodsTable.Fields.Spec_Etc.Name,
            PodsTable.Fields.Spec_AccountId.Name,
            PodsTable.Fields.Status_Etc.Name,
            PodsTable.Fields.Labels.Name,
            ytConnector->GetTablePath(&PodsTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name,
            PodsTable.Fields.Spec_EnableScheduling.Name);
    }

    std::unique_ptr<TPod> ParsePodFromRow(TUnversionedRow row)
    {
        TObjectId podId;
        TObjectId podSetId;
        NServer::NObjects::NProto::TMetaEtc metaEtc;
        TObjectId nodeId;
        NServer::NObjects::NProto::TPodSpecEtc specEtc;
        TObjectId accountId;
        NServer::NObjects::NProto::TPodStatusEtc statusEtc;
        TYsonString labels;
        FromUnversionedRow(
            row,
            &podId,
            &podSetId,
            &metaEtc,
            &nodeId,
            &specEtc,
            &accountId,
            &statusEtc,
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
            specEtc.ip6_address_requests(),
            specEtc.ip6_subnet_requests(),
            std::move(*specEtc.mutable_node_filter()),
            std::move(*statusEtc.mutable_eviction()));
    }


    TString GetResourceQueryString()
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

    std::unique_ptr<TResource> ParseResourceFromRow(TUnversionedRow row)
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
};

////////////////////////////////////////////////////////////////////////////////

IClusterReaderPtr CreateClusterReader(TBootstrap* bootstrap)
{
    return New<TClusterReader>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
