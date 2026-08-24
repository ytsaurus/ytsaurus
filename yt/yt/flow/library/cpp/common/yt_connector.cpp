#include "yt_connector.h"

#include "control_table.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/flow/library/cpp/client/public.h>
#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NClient::NCache;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(ICommonYTConnector);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

void EnsureSameTabletCellBundle(const std::vector<TInternalTableInfo>& tables)
{
    THashSet<std::string_view> bundleNames;
    for (const auto& table : tables) {
        bundleNames.insert(table.TabletCellBundle);
    }
    THROW_ERROR_EXCEPTION_IF(bundleNames.empty(),
        "No internal flow tables found to determine the bundle");
    THROW_ERROR_EXCEPTION_UNLESS(bundleNames.size() == 1,
        "All internal flow tables must be in the same bundle")
        .With("bundle_names", bundleNames);
}

void EnsureSameTableType(const std::vector<TInternalTableInfo>& tables)
{
    THashSet<NObjectClient::EObjectType> tableTypes;
    for (const auto& table : tables) {
        tableTypes.insert(table.Type);
    }
    THROW_ERROR_EXCEPTION_IF(tableTypes.empty(),
        "No internal flow tables found to determine the type");
    THROW_ERROR_EXCEPTION_UNLESS(tableTypes.size() == 1,
        "All internal flow tables must be the same type")
        .With("table_types", tableTypes);
}

// All tables on a chaos installation's master cluster are chaos-replicated.
bool IsChaosTableLayout(const std::vector<TInternalTableInfo>& tables)
{
    EnsureSameTableType(tables);
    return tables[0].Type == NObjectClient::EObjectType::ChaosReplicatedTable;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TCommonYTConnector
    : public ICommonYTConnector
{
public:
    TCommonYTConnector(
        IClientsCachePtr clientsCache,
        NYPath::TRichYPath pipelinePath)
        : ClientsCache_(std::move(clientsCache))
        , PipelinePath_(std::move(pipelinePath))
        , Client_(ClientsCache_->GetClient(*PipelinePath_.GetCluster()))
    { }

    TFuture<TPipelineAttributes> GetPipelineAttributes() override
    {
        TGetNodeOptions getOptions;
        getOptions.Attributes = {
            PipelineFormatVersionAttribute,
            MonitoringProjectAttribute,
            MonitoringClusterAttribute,
        };
        auto cypressFuture = Client_->GetNode(PipelinePath_.GetPath(), getOptions);

        auto flowControlPath = YPathJoin(PipelinePath_.GetPath(), FlowControlTableName);
        auto leaderInfoFuture = TControlTable::Read(Client_, flowControlPath, LeaderControllerKey);

        // Wait for both the Cypress attributes and the flow_control lookup, then assemble the result
        // in a single continuation (AllSucceeded avoids nesting one Apply inside another).
        return AllSucceeded(std::vector<TFuture<void>>{cypressFuture.AsVoid(), leaderInfoFuture.AsVoid()})
            .Apply(BIND([cypressFuture = std::move(cypressFuture), leaderInfoFuture = std::move(leaderInfoFuture)] {
                // Both futures are set and successful once AllSucceeded resolves.
                auto node = ConvertToNode(cypressFuture.GetOrCrash().ValueOrThrow());

                // The value is the YSON-serialized leader node info (see LeaderControllerKey);
                // pull the RPC address out of it. A missing row/field leaves the address empty.
                std::string leaderControllerAddress;
                if (const auto& leaderInfoYson = leaderInfoFuture.GetOrCrash().ValueOrThrow()) {
                    auto leaderInfo = ConvertToNode(*leaderInfoYson);
                    if (leaderInfo->GetType() == ENodeType::Map) {
                        leaderControllerAddress = leaderInfo->AsMap()
                            ->FindChildValue<std::string>(std::string(LeaderControllerRpcAddressField))
                            .value_or("");
                    }
                }

                return TPipelineAttributes{
                    .LeaderControllerAddress = std::move(leaderControllerAddress),
                    .PipelineFormatVersion = node->Attributes().Get<i64>(PipelineFormatVersionAttribute),
                    .MonitoringProject = node->Attributes().Get<std::string>(MonitoringProjectAttribute, ""),
                    .MonitoringCluster = node->Attributes().Get<std::string>(MonitoringClusterAttribute, ""),
                };
            }));
    }

    TFuture<TFlowTablesBundleInfo> GetFlowTablesBundle() override
    {
        if (auto cached = CachedFlowTablesBundle_.Load()) {
            return MakeFuture(*cached);
        }

        // Some tables may be absent on pipelines whose layout predates them; such tables are
        // skipped, mirroring GetFlowTablesCellTag in node.cpp.
        std::vector<TFuture<TYsonString>> tableFutures;
        tableFutures.reserve(InternalFlowTables.size());
        for (const auto& tableName : InternalFlowTables) {
            auto tablePath = YPathJoin(PipelinePath_.GetPath(), tableName);
            tableFutures.push_back(Client_->GetNode(tablePath, {.Attributes = {"type", "tablet_cell_bundle"}}));
        }

        auto client = Client_;
        // AllSet collects per-table results as TErrorOr without short-circuiting, so an absent
        // table (ResolveError) can be skipped instead of failing the whole resolution.
        return AllSet(std::move(tableFutures))
            .Apply(BIND([client] (const std::vector<TErrorOr<TYsonString>>& tableResults) {
                std::vector<NDetail::TInternalTableInfo> tables;
                tables.reserve(tableResults.size());
                for (const auto& tableResult : tableResults) {
                    if (tableResult.FindMatching(NYTree::EErrorCode::ResolveError)) {
                        // The table is absent on this pipeline; all internal tables share one
                        // bundle, so a missing one can be skipped.
                        continue;
                    }
                    auto tableNode = ConvertTo<INodePtr>(tableResult.ValueOrThrow());
                    const auto& attributes = tableNode->Attributes();
                    tables.push_back(NDetail::TInternalTableInfo{
                        .Type = attributes.Get<NObjectClient::EObjectType>("type"),
                        .TabletCellBundle = attributes.Get<std::string>("tablet_cell_bundle"),
                    });
                }

                THROW_ERROR_EXCEPTION_IF(tables.empty(), "No internal flow tables found");
                EnsureSameTabletCellBundle(tables);

                auto bundleName = tables[0].TabletCellBundle;

                if (IsChaosTableLayout(tables)) {
                    // Chaos cell bundles use the master cluster's primary cell tag as their clock cluster tag.
                    return client->GetNode("//sys/@primary_cell_tag")
                        .Apply(BIND([bundleName = std::move(bundleName)] (const TYsonString& cellTagYson) mutable {
                            return TFlowTablesBundleInfo{
                                .Bundle = std::move(bundleName),
                                .ClockClusterTag = NObjectClient::TCellTag(ConvertTo<ui16>(cellTagYson)),
                            };
                        }));
                }

                // A pipeline living on a single cluster may deliberately be pinned to a clock
                // other than its own, so keep taking its tag from the bundle.
                auto bundlePath = Format("//sys/tablet_cell_bundles/%v", ToYPathLiteral(bundleName));
                return client->GetNode(bundlePath, {.Attributes = {"options"}})
                    .Apply(BIND([bundleName = std::move(bundleName)] (const TYsonString& bundleYson) mutable {
                        auto bundleNode = ConvertTo<INodePtr>(bundleYson);
                        std::optional<NObjectClient::TCellTag> clockClusterTag;
                        if (auto bundleOptions = bundleNode->Attributes().Find<IMapNodePtr>("options")) {
                            if (auto cellTag = bundleOptions->FindChild("clock_cluster_tag")) {
                                clockClusterTag = NObjectClient::TCellTag(ConvertTo<ui64>(cellTag));
                            }
                        }
                        return TFlowTablesBundleInfo{
                            .Bundle = std::move(bundleName),
                            .ClockClusterTag = clockClusterTag,
                        };
                    }));
            }))
            .Apply(BIND([this, this_ = MakeStrong(this)] (const TFlowTablesBundleInfo& bundleInfo) {
                // Cache the successful result so the flow execute path does not re-issue the
                // YT queries and stall. Failures are not cached, so a transient error is retried.
                CachedFlowTablesBundle_.Store(bundleInfo);
                return bundleInfo;
            }));
    }

    NApi::IClientPtr GetClient() override
    {
        return Client_;
    }

    NYPath::TRichYPath GetPipelinePath() override
    {
        return PipelinePath_;
    }

    IClientsCachePtr GetClientsCache() override
    {
        return ClientsCache_;
    }

private:
    const IClientsCachePtr ClientsCache_;
    const NYPath::TRichYPath PipelinePath_;
    const IClientPtr Client_;

    NThreading::TAtomicObject<std::optional<TFlowTablesBundleInfo>> CachedFlowTablesBundle_;
};

////////////////////////////////////////////////////////////////////////////////

ICommonYTConnectorPtr CreateCommonYTConnector(
    IClientsCachePtr clientsCache,
    NYPath::TRichYPath pipelinePath)
{
    return New<TCommonYTConnector>(
        std::move(clientsCache),
        std::move(pipelinePath));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
