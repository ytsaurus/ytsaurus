#include "snapshot_exporter.h"
#include "hydra_facade.h"

#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/helpers.h>
#include <yt/yt/server/master/cypress_server/node_detail.h>
#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/public.h>


namespace NYT::NCellMaster {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NChunkServer;
using namespace NConcurrency;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

//////////////////////////////////////////////////////////////////////////

namespace {

DECLARE_REFCOUNTED_CLASS(TExportArgumentsConfig)

class TExportArgumentsConfig
    : public TYsonStruct
{
public:
    std::optional<i64> JobIndex;
    std::optional<i64> JobCount;
    std::optional<i64> LowerIndex;
    std::optional<i64> UpperIndex;
    std::vector<TString> Types;
    std::vector<TString> Attributes;
    std::vector<TString> AdditionalAttributes;
    bool CalculateExtendedBranchStatistics;

    REGISTER_YSON_STRUCT(TExportArgumentsConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("job_index", &TThis::JobIndex)
            .GreaterThanOrEqual(0)
            .Default();
        registrar.Parameter("job_count", &TThis::JobCount)
            .GreaterThan(0)
            .Default();
        registrar.Parameter("lower_index", &TThis::LowerIndex)
            .GreaterThanOrEqual(0)
            .Default();
        registrar.Parameter("upper_index", &TThis::UpperIndex)
            .GreaterThan(0)
            .Default();
        registrar.Parameter("types", &TThis::Types)
            .Default();
        registrar.Parameter("attributes", &TThis::Attributes)
            .Default();
        registrar.Parameter("additional_attributes", &TThis::AdditionalAttributes)
            .Default();
        registrar.Parameter("calculate_extended_branch_statistics", &TThis::CalculateExtendedBranchStatistics)
            .Default(true);

        registrar.Postprocessor([] (TThis* config) {
            if ((config->JobIndex.has_value() || config->JobCount.has_value()) &&
                (config->LowerIndex.has_value() || config->UpperIndex.has_value()))
            {
                THROW_ERROR_EXCEPTION("Invalid export config: only one way of iterating (through keys or through jobs) may be set");
            }
            if (config->LowerIndex.has_value() && config->UpperIndex.has_value() &&
                config->LowerIndex.value() >= config->UpperIndex.value())
            {
                THROW_ERROR_EXCEPTION("Invalid export config: \"lower_index\" must be less than \"upper_index\"");
            }
            if (config->JobIndex.has_value() ^ config->JobCount.has_value()) {
                THROW_ERROR_EXCEPTION("Invalid export config: \"job_index\" and \"job_count\" can only be defined together");
            }
            if (config->JobIndex.has_value() && config->JobIndex.value() >= config->JobCount.value()) {
                THROW_ERROR_EXCEPTION("Invalid export config: \"job_count\" must be greater than \"job_index\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TExportArgumentsConfig)

//////////////////////////////////////////////////////////////////////////

static const std::vector<TString> PresetKeys = {
    "access_time", \
    "account", \
    "acl", \
    "chunk_count", \
    "chunk_format", \
    "chunk_row_count", \
    "compressed_data_size", \
    "compression_codec", \
    "creation_time", \
    "data_weight", \
    "dynamic", \
    "erasure_codec", \
    "external", \
    "hunk_media", \
    "hunk_primary_medium", \
    "id", \
    "in_memory_mode", \
    "inherit_acl", \
    "media", \
    "modification_time", \
    "monitor_table_statistics", \
    "optimize_for", \
    "owner", \
    "parent_id", \
    "primary_medium", \
    "ref_counter", \
    "replication_factor", \
    "resource_usage", \
    "revision", \
    "row_count", \
    "schema_attributes", \
    "schema_mode", \
    "schema", \
    "sorted", \
    "tablet_cell_bundle", \
    "target_path", \
    "type", \
    "uncompressed_data_size", \
    "user_attribute_keys", \
    "versioned_resource_usage"
};

void ExportNode(
    TYsonWriter* writer,
    TCypressNode* node,
    const TExportArgumentsConfigPtr& config,
    const ICypressNodeProxyPtr& nodeProxy,
    const std::vector<TString>& keys,
    const std::vector<NCypressClient::TNodeId>& extraNodesHeld,
    const ICypressManagerPtr& cypressManager)
{
    auto transaction = node->GetTransaction();
    BuildYsonFluently(writer)
        .BeginMap()
        .DoIf(transaction, [&] (TFluentMap fluent) {
            fluent
                .Item("cypress_transaction_id")
                .Value(transaction->GetId());
            fluent
                .Item("cypress_transaction_title")
                .Value(transaction->GetTitle());
            fluent
                .Item("cypress_transaction_timestamp")
                .Value(TimestampFromTransactionId(transaction->GetId()));
        })
        .DoIf(transaction && transaction->GetParent(), [&] (TFluentMap fluent) {
            // For snapshot branch, originator and parent might differ.
            auto* originatorTransaction = node->GetOriginator()->GetTransaction();
            auto originatorTransactionId = originatorTransaction ? originatorTransaction->GetId() : NullObjectId;

            fluent
                .Item("originator_cypress_transaction_id")
                .Value(originatorTransactionId);
        })
        .DoIf(config->CalculateExtendedBranchStatistics && !node->GetParent() && node->IsTrunk(), [&] (TFluentMap fluent) {
            auto nodePath = cypressManager->GetNodePath(node, nullptr);
            if (nodePath != "/") {
                fluent
                    .Item("orphaned")
                    .Value(true);
            }
        })
        .DoIf(!extraNodesHeld.empty(), [&] (TFluentMap fluent) {
            fluent
                .Item("children_held")
                .Value(extraNodesHeld);
        })
        .Do([&] (TFluentMap fluent) {
            const auto& nodeAttributes = nodeProxy->Attributes();

            fluent
                .Item("path")
                .Value(cypressManager->GetNodePath(node->GetTrunkNode(), node->GetTransaction()));

            fluent
                .DoFor(keys, [&] (TFluentMap fluent, const TString& key) {
                    auto getAttribute = [&] {
                        auto internedKey = TInternedAttributeKey::Lookup(key);
                        if (internedKey != InvalidInternedAttribute) {
                            if (auto result = nodeProxy->FindBuiltinAttribute(internedKey)) {
                                return result;
                            }
                            if (auto future = nodeProxy->GetBuiltinAttributeAsync(internedKey)) {
                                auto resultOrError = WaitFor(future);
                                if (resultOrError.IsOK()) {
                                    return resultOrError.Value();
                                }
                            }
                        }

                        return nodeAttributes.FindYson(key);
                    };

                    auto attribute = getAttribute();
                    fluent
                        .DoIf(static_cast<bool>(attribute), [&] (TFluentMap fluent) {
                            auto node = ConvertToNode(attribute);
                            // Attributes are forbidden on zero depth,
                            // but we still want to have them (e.g. for schema).
                            if (!node->Attributes().ListKeys().empty()) {
                                fluent
                                    .Item(key + "_attributes")
                                    .Value(node->Attributes());
                                node->MutableAttributes()->Clear();
                            }
                            fluent
                                .Item(key)
                                .Value(node);
                        });
                });
        })
        .EndMap();
}

void DoExportSnapshot(
    TBootstrap* bootstrap,
    const TExportArgumentsConfigPtr& config,
    std::vector<TString> searchedKeys,
    const THashSet<EObjectType>& searchedTypes)
{
    const auto& cypressManager = bootstrap->GetCypressManager();

    std::vector<TCypressNode*> sortedNodes;
    sortedNodes.reserve(cypressManager->Nodes().size());
    for (auto [nodeId, node] : cypressManager->Nodes()) {
        // Nodes under transaction have zero RefCounter and are not considered alive.
        if (IsObjectAlive(node) || node->GetTrunkNode() != node) {
            sortedNodes.emplace_back(node);
        }
    }

    auto originatorLess = [] (TCypressNode* lhs, TCypressNode* rhs) {
        if (lhs->GetId() == rhs->GetId()) {
            if (!lhs->GetTransaction() || !rhs->GetTransaction()) {
                if (!lhs->GetTransaction() == !rhs->GetTransaction()) {
                    return false;
                }
                return lhs->GetTransaction() != nullptr;
            }

            auto lhsTransactionTimestamp = TimestampFromTransactionId(lhs->GetTransaction()->GetId());
            auto rhsTransactionTimestamp = TimestampFromTransactionId(rhs->GetTransaction()->GetId());
            return lhsTransactionTimestamp > rhsTransactionTimestamp;
        }

        return lhs->GetId() < rhs->GetId();
    };
    std::sort(sortedNodes.begin(), sortedNodes.end(), originatorLess);

    auto sortedNodesSize = std::ssize(sortedNodes);

    // We process [lowerIndex, upperIndex) interval,
    // where lowerIndex and upperIndex denote indices of corresponding sorted keys of NodeMap_.
    auto lowerIndex = config->LowerIndex.has_value() ? config->LowerIndex : 0;
    auto upperIndex = config->UpperIndex.has_value() ? config->UpperIndex : sortedNodesSize;
    // Other option if we currently don't know total number of nodes.
    auto jobIndex = config->JobIndex;
    auto jobCount = config->JobCount;

    if (jobIndex.has_value()) {
        int nodesPerJob = DivCeil(sortedNodesSize, *jobCount);
        lowerIndex = *jobIndex * nodesPerJob;
        upperIndex = *lowerIndex + nodesPerJob;
    }
    upperIndex = std::min(*upperIndex, sortedNodesSize);

    // Skipping nodes from a previous job range.
    int nodeIdx = *lowerIndex;
    if (nodeIdx > 0) {
        while (!sortedNodes[nodeIdx - 1]->IsTrunk() && nodeIdx < sortedNodesSize) {
            ++nodeIdx;
        }
    }

    if (nodeIdx >= sortedNodesSize) {
        return;
    }

    do {
        auto node = sortedNodes[nodeIdx];
        ++nodeIdx;

        auto nodeType = node->GetType();
        // Skip sys_node. Such node has attributes that cannot be exported
        // (e.g. chunk_replicator_enabled) due to RequireLeader checks.
        if (nodeType == EObjectType::SysNode) {
            continue;
        }
        if (!searchedTypes.empty() && !searchedTypes.contains(nodeType)) {
            continue;
        }

        std::vector<NCypressClient::TNodeId> extraNodesHeld;
        if (config->CalculateExtendedBranchStatistics && !node->IsTrunk() && nodeType == EObjectType::MapNode) {
            auto* mapNode = node->As<TCypressMapNode>();
            const auto& nodeChildren = mapNode->ChildToKey();
            auto isSnapshotBranch = mapNode->GetLockMode() == ELockMode::Snapshot;

            auto* originator = node->GetOriginator()->As<TCypressMapNode>();
            const auto& originatorChildren = originator->ChildToKey();

            for (const auto& [child, _] : nodeChildren) {
                // Non-snapshot branches only hold changes, i.e. deltas.
                if (!isSnapshotBranch || !originatorChildren.contains(child)) {
                    extraNodesHeld.push_back(child->GetId());
                }
            }
        }

        auto writer = std::make_unique<TYsonWriter>(
            &Cout,
            EYsonFormat::Text,
            EYsonType::Node);

        auto nodeProxy = cypressManager->GetNodeProxy(node->GetTrunkNode(), node->GetTransaction());
        const auto& nodeAttributes = nodeProxy->Attributes();
        auto keys = searchedKeys.empty() ? nodeAttributes.ListKeys() : searchedKeys;

        ExportNode(
            writer.get(),
            node,
            config,
            nodeProxy,
            keys,
            extraNodesHeld,
            cypressManager);

        writer->Flush();
        Cout << ";" << Endl;
    } while (nodeIdx < sortedNodesSize &&
            (nodeIdx < *upperIndex || !sortedNodes[nodeIdx - 1]->IsTrunk()));
}

//////////////////////////////////////////////////////////////////////////

} // namespace

//////////////////////////////////////////////////////////////////////////

void ExportSnapshot(TBootstrap* bootstrap, const TString& configPath)
{
    auto config = ConvertTo<TExportArgumentsConfigPtr>(TYsonString(configPath));

    auto searchedAttributes = config->Attributes.empty()
        ? PresetKeys
        : std::move(config->Attributes);

    for (auto& attribute : config->AdditionalAttributes) {
        searchedAttributes.push_back(attribute);
    }

    THashSet<EObjectType> searchedTypes;
    searchedTypes.reserve(config->Types.size());
    for (const auto& typeNode : config->Types) {
        try {
            searchedTypes.insert(ConvertTo<EObjectType>(typeNode));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Invalid export config: unable to parse type %Qv", typeNode)
                << ex;
        }
    }

    BIND(&DoExportSnapshot, bootstrap, config, searchedAttributes, searchedTypes)
        .AsyncVia(bootstrap->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default))
        .Run()
        .Get()
        .ThrowOnError();
}

//////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
