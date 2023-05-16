#include "snapshot_exporter.h"
#include "hydra_facade.h"

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/node.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/public.h>


namespace NYT::NCellMaster {

using namespace NYTree;
using namespace NYson;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NConcurrency;

//////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(EPresetAttributes,
    ((Small)      (001))
    ((Large)      (002))
);

DECLARE_REFCOUNTED_CLASS(TExportArgumentsConfig)

class TExportArgumentsConfig
    : public TYsonStruct
{
public:
    std::optional<int> JobIndex;
    std::optional<int> JobCount;
    std::optional<int> LowerIndex;
    std::optional<int> UpperIndex;
    std::vector<TString> UserTypes;
    std::vector<TString> UserAttributes;
    std::optional<EPresetAttributes> PresetAttributes;

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
        registrar.Parameter("types", &TThis::UserTypes)
            .Default();
        registrar.Parameter("attributes", &TThis::UserAttributes)
            .Default();
        registrar.Parameter("preset_attributes", &TThis::PresetAttributes)
            .Default();

        registrar.Postprocessor([] (TThis* config) {
            if (config->PresetAttributes.has_value() ^ config->UserAttributes.empty()) {
                THROW_ERROR_EXCEPTION("Invalid export config: exactly one key \"attributes\" or \"preset_attributes\" must be defined");
            }
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
    "chunk_format", \
    "hunk_erasure_codec",

static const std::vector<TString> PresetKeysSmall = {
    BASIC_KEYS
};

static const std::vector<TString> PresetKeysLarge = {
    BASIC_KEYS
    "schema",
    "media",
    "effective_acl"
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
    const auto& cypressManager = bootstrap->GetCypressManager();

    std::vector<TCypressNode*> sortedNodes;
    sortedNodes.reserve(cypressManager->Nodes().size());
    for (auto [nodeId, node] : cypressManager->Nodes()) {
        // Nodes under transaction have zero RefCounter and are not considered alive.
        if (IsObjectAlive(node) || node->GetTrunkNode() != node) {
            sortedNodes.emplace_back(node);
        }
    }
    std::sort(sortedNodes.begin(), sortedNodes.end(), TObjectIdComparer());

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

        auto writer = std::make_unique<TYsonWriter>(
            &Cout,
            EYsonFormat::Text,
            EYsonType::Node);

        auto transaction = node->GetTransaction();
        BuildYsonFluently(writer.get())
            .BeginMap()
            .DoIf(transaction, [&] (TFluentMap fluent) {
                fluent
                    .Item("cypress_transaction_id")
                    .Value(transaction->GetId());
            })
            .Do([&] (TFluentMap fluent) {
                auto nodeProxy = cypressManager->GetNodeProxy(node->GetTrunkNode(), node->GetTransaction());
                const auto& nodeAttributes = nodeProxy->Attributes();
                auto keys = searchedKeys.empty() ? nodeAttributes.ListKeys() : searchedKeys;

                fluent
                    .Item("path")
                    .Value(cypressManager->GetNodePath(node->GetTrunkNode(), node->GetTransaction()));

                fluent
                    .DoFor(keys, [&] (TFluentMap fluent, const TString &key) {
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
            *searchedKeys = PresetKeysSmall;
        } else if (*config->PresetAttributes == EPresetAttributes::Large) {
            *searchedKeys = PresetKeysLarge;
        }
    } else {
        *searchedKeys = std::move(config->UserAttributes);
    }

    searchedTypes->reserve(config->UserTypes.size());
    for (const auto& typeNode : config->UserTypes) {
        try {
            searchedTypes->insert(ConvertTo<EObjectType>(typeNode));
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Invalid export config: unable to parse type %Qv", typeNode)
                << ex;
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

    bootstrap->LoadSnapshotOrThrow(snapshotPath, false, false, TString());
    BIND(&DoExportSnapshot, bootstrap, lowerIndex, upperIndex, jobIndex, jobCount, searchedAttributes, searchedTypes)
        .AsyncVia(bootstrap->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default))
        .Run()
        .Get()
        .ThrowOnError();
}

//////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
