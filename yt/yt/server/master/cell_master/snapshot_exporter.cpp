#include "snapshot_exporter.h"
#include "hydra_facade.h"

#include <yt/server/master/cypress_server/cypress_manager.h>
#include <yt/server/master/cypress_server/node.h>

#include <yt/core/misc/numeric_helpers.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/public.h>


namespace NYT::NCellMaster {

using namespace NYTree;
using namespace NYson;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NObjectServer;

//////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(EPresetAttributes,
    ((Small)      (001))
    ((Large)      (002))
);

DECLARE_REFCOUNTED_CLASS(TExportArgumentsConfig)

class TExportArgumentsConfig
    : public TYsonSerializable
{
public:
    std::optional<int> JobIndex;
    std::optional<int> JobCount;
    std::optional<int> LowerIndex;
    std::optional<int> UpperIndex;
    std::vector<TString> UserTypes;
    std::vector<TString> UserAttributes;
    std::optional<EPresetAttributes> PresetAttributes;

    TExportArgumentsConfig()
    {
        RegisterParameter("job_index", JobIndex)
            .GreaterThanOrEqual(0)
            .Default();
        RegisterParameter("job_count", JobCount)
            .GreaterThan(0)
            .Default();
        RegisterParameter("lower_index", LowerIndex)
            .GreaterThanOrEqual(0)
            .Default();
        RegisterParameter("upper_index", UpperIndex)
            .GreaterThan(0)
            .Default();
        RegisterParameter("types", UserTypes)
            .Default();
        RegisterParameter("attributes", UserAttributes)
            .Default();
        RegisterParameter("preset_attributes", PresetAttributes)
            .Default();

        RegisterPostprocessor([&] () {
            if (PresetAttributes.has_value() ^ UserAttributes.empty()) {
                THROW_ERROR_EXCEPTION("Invalid export config: exactly one key \"attributes\" or \"preset_attributes\" must be defined");
            }
            if ((JobIndex.has_value() || JobCount.has_value()) &&
                (LowerIndex.has_value() || UpperIndex.has_value()))
            {
                THROW_ERROR_EXCEPTION("Invalid export config: only one way of iterating (through keys or through jobs) may be set");
            }
            if (LowerIndex.has_value() && UpperIndex.has_value() &&
                LowerIndex.value() >= UpperIndex.value())
            {
                THROW_ERROR_EXCEPTION("Invalid export config: \"lower_index\" must be less than \"upper_index\"");
            }
            if (JobIndex.has_value() ^ JobCount.has_value()) {
                THROW_ERROR_EXCEPTION("Invalid export config: \"job_index\" and \"job_count\" can only be defined together");
            }
            if (JobIndex.has_value() && JobIndex.value() >= JobCount.value()) {
                THROW_ERROR_EXCEPTION("Invalid export config: \"job_count\" must be greater than \"job_index\"");
            }
        });
    };
};

DEFINE_REFCOUNTED_TYPE(TExportArgumentsConfig)

//////////////////////////////////////////////////////////////////////////

#define BASIC_KEYS \
    "id", \
    "type", \
    "ref_counter", \
    "external", \
    "owner", \
    "parent_id", \
    "creation_time", \
    "modification_time", \
    "access_time", \
    "account", \
    "chunk_count", \
    "uncompressed_data_size", \
    "compressed_data_size", \
    "data_weight", \
    "replication_factor", \
    "primary_medium", \
    "compression_codec", \
    "erasure_codec", \
    "chunk_row_count", \
    "dynamic", \
    "tablet_cell_bundle", \
    "in_memory_mode", \
    "optimize_for", \

static const std::vector<TString> preset_keys_small = {
    BASIC_KEYS
};

static const std::vector<TString> preset_keys_large = {
    BASIC_KEYS
    "schema",
    "media"
};

#undef BASIC_KEYS

void DoExportSnapshot(
    TBootstrap* bootstrap,
    std::optional<int> lowerIndex,
    std::optional<int> upperIndex,
    std::optional<int> jobIndex,
    std::optional<int> jobCount,
    const std::vector<TString>& searchedKeys,
    const THashSet<EObjectType>& searchedTypes)
{
    const auto& objectManager = bootstrap->GetObjectManager();
    const auto& cypressManager = bootstrap->GetCypressManager();

    std::vector<TCypressNode*> sortedNodes;
    sortedNodes.reserve(cypressManager->Nodes().size());
    for (const auto& [nodeId, node] : cypressManager->Nodes()) {
        // Nodes under transaction have zero RefCounter and are not considered alive.
        if (IsObjectAlive(node) || node->GetTrunkNode() != node) {
            sortedNodes.emplace_back(node);
        }
    }
    std::sort(sortedNodes.begin(), sortedNodes.end(), TObjectRefComparer::Compare);

    if (!lowerIndex.has_value()) {
        lowerIndex = 0;
    }
    if (!upperIndex.has_value()) {
        upperIndex = sortedNodes.size();
    }
    if (jobIndex.has_value()) {
        int nodesPerJob = DivCeil(static_cast<int>(sortedNodes.size()), *jobCount);
        lowerIndex = *jobIndex * nodesPerJob;
        upperIndex = *lowerIndex + nodesPerJob;
    }
    upperIndex = std::min(*upperIndex, static_cast<int>(sortedNodes.size()));

    for (int nodeIdx = *lowerIndex; nodeIdx < *upperIndex; ++nodeIdx) {
        auto node = sortedNodes[nodeIdx];

        auto nodeType = node->GetType();
        // Skip sys_node. Such node has attributes that cannot be exported
        // (e.g. chunk_replicator_enabled) due to RequireLeader checks.
        if (nodeType == EObjectType::SysNode) {
            continue;
        }
        if (!searchedTypes.empty() && !searchedTypes.contains(nodeType)) {
            continue;
        }

        auto writer = CreateYsonWriter(
            &Cout,
            EYsonFormat::Text,
            EYsonType::Node,
            /* enableRaw */ false,
            /* booleanAsString */ false);

        auto transaction = node->GetTransaction();
        BuildYsonFluently(writer.get())
            .BeginMap()
            .DoIf(transaction, [&] (TFluentMap fluent) {
                fluent
                    .Item("cypress_transaction_id")
                    .Value(transaction->GetId());
            })
            .Do([&] (TFluentMap fluent) {
                auto nodeProxy = objectManager->GetProxy(node->GetTrunkNode(), node->GetTransaction());
                const auto& nodeAttributes = nodeProxy->Attributes();
                auto keys = searchedKeys.empty() ? nodeAttributes.ListKeys() : searchedKeys;

                fluent
                    .Item("path")
                    .Value(cypressManager->GetNodePath(node->GetTrunkNode(), node->GetTransaction()));

                fluent
                    .DoFor(keys, [&] (TFluentMap fluent, const TString &key) {
                        fluent
                            .Do([&] (TFluentMap fluent) {
                                // Safely using FindYson because primary master doesn't store some attributes of dynamic tables.
                                auto result = nodeAttributes.FindYson(key);
                                fluent
                                    .DoIf(static_cast<bool>(result), [&] (TFluentMap fluent) {
                                        auto node = ConvertToNode(result);
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
                    });
            })
            .EndMap();

        writer->Flush();
        Cout << ";" << Endl;
    }
}

void ParseAndValidate(
    const TString& exportConfig,
    THashSet<EObjectType>* searchedTypes,
    std::vector<TString>* searchedKeys,
    std::optional<int>* lowerIndex,
    std::optional<int>* upperIndex,
    std::optional<int>* jobIndex,
    std::optional<int>* jobCount)
{
    auto config = ConvertTo<TExportArgumentsConfigPtr>(TYsonString(exportConfig));

    *lowerIndex = config->LowerIndex;
    *upperIndex = config->UpperIndex;
    *jobIndex = config->JobIndex;
    *jobCount = config->JobCount;

    if (config->PresetAttributes.has_value()) {
        if (*config->PresetAttributes == EPresetAttributes::Small) {
            *searchedKeys = preset_keys_small;
        } else if (*config->PresetAttributes == EPresetAttributes::Large) {
            *searchedKeys = preset_keys_large;
        }
    } else {
        *searchedKeys = std::move(config->UserAttributes);
    }

    searchedTypes->reserve(config->UserTypes.size());
    for (const auto& typeNode : config->UserTypes) {
        try {
            searchedTypes->insert(ConvertTo<EObjectType>(typeNode));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Invalid export config: unable to parse type %Qv", typeNode) << TError(ex);
        }
    }
}

//////////////////////////////////////////////////////////////////////////

} // namespace

//////////////////////////////////////////////////////////////////////////

void ExportSnapshot(TBootstrap* bootstrap, const TString& snapshotPath, const TString& configPath)
{
    THashSet<EObjectType> searchedTypes;
    std::vector<TString> searchedAttributes;
    // We process [lowerIndex, upperIndex) interval,
    // where lowerIndex and upperIndex denote indices of corresponding sorted keys of NodeMap_.
    std::optional<int> lowerIndex;
    std::optional<int> upperIndex;
    // Other option if we currently don't know total number of nodes.
    std::optional<int> jobIndex;
    std::optional<int> jobCount;

    ParseAndValidate(configPath, &searchedTypes, &searchedAttributes, &lowerIndex, &upperIndex, &jobIndex, &jobCount);

    bootstrap->TryLoadSnapshot(snapshotPath, false, false, TString());
    BIND(&DoExportSnapshot, bootstrap, lowerIndex, upperIndex, jobIndex, jobCount, searchedAttributes, searchedTypes)
        .AsyncVia(bootstrap->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default))
        .Run()
        .Get()
        .ThrowOnError();
}

//////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
