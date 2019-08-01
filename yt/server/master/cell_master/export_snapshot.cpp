#include "export_snapshot.h"
#include "hydra_facade.h"

#include <yt/core/ytree/fluent.h>
#include <yt/server/master/cypress_server/cypress_manager.h>

namespace NYT::NCellMaster {

using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NObjectServer;

//////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(EPresetAttributes,
    ((Small)      (001))
    ((Large)      (002))
);

DECLARE_REFCOUNTED_CLASS(ExportArgumentsConfig)

class ExportArgumentsConfig
    : public TYsonSerializable {
public:
    std::optional<int> LowerIndex;
    std::optional<int> UpperIndex;
    std::vector<TString> UserTypes;
    std::vector<TString> UserAttributes;
    std::optional<EPresetAttributes> PresetAttributes;

    ExportArgumentsConfig() {
        RegisterParameter("lower_index", LowerIndex)
                .Default();
        RegisterParameter("upper_index", UpperIndex)
                .Default();
        RegisterParameter("types", UserTypes)
                .Default();
        RegisterParameter("attributes", UserAttributes)
                .Default();
        RegisterParameter("preset_attributes", PresetAttributes)
                .Default();
    };
};

DEFINE_REFCOUNTED_TYPE(ExportArgumentsConfig)

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
    "path", \

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
    const std::vector<TString>& searchedKeys,
    const THashSet<EObjectType>& searchedTypes)
{
    const auto& objectManager = bootstrap->GetObjectManager();
    const auto& cypressManager = bootstrap->GetCypressManager();

    auto sortedValues = GetValuesSortedByKey(cypressManager->Nodes());

    if (!lowerIndex.has_value()) {
        lowerIndex = 0;
    }
    if (!upperIndex.has_value()) {
        upperIndex = sortedValues.size();
    }
    upperIndex = std::min(upperIndex.value(), static_cast<int>(sortedValues.size()));

    for (int nodeIdx = lowerIndex.value(); nodeIdx < upperIndex.value(); ++nodeIdx) {
        auto node = sortedValues[nodeIdx];
        if (!IsObjectAlive(node)) {
            continue;
        }

        auto nodeType = node->GetType();
        // Skip sys_node. Such node has attributes that cannot be exported (e.g. chunk_replicator_enabled) due to RequireLeader checks.
        if (nodeType == EObjectType::SysNode) {
            continue;
        }
        if (!searchedTypes.empty() && !searchedTypes.contains(nodeType)) {
            continue;
        }

        auto nodeProxy = objectManager->GetProxy(node, nullptr);
        const auto& nodeAttributes = nodeProxy->Attributes();
        std::vector<TString> keys = searchedKeys.empty() ? nodeAttributes.List() : searchedKeys;
        auto writer = CreateYsonWriter(
            &Cout,
            EYsonFormat::Text,
            EYsonType::Node,
            /* enableRaw */ false,
            /* booleanAsString */ false);

        BuildYsonFluently(writer.get())
            .DoMapFor(keys, [&] (TFluentMap fluent, const TString& key) {
                fluent
                    .DoIf(key == "path", [&] (TFluentMap fluent) {
                        fluent
                            .Item(key)
                            .Value(cypressManager->GetNodePath(node, nullptr));
                    })
                    .DoIf(key != "path", [&] (TFluentMap fluent) {
                        // Safely using FindYson because primary master doesn't store some attributes of dynamic tables.
                        auto result = nodeAttributes.FindYson(key);
                        fluent
                            .DoIf(static_cast<bool>(result), [&] (TFluentMap fluent) {
                                  fluent
                                      .Item(key)
                                      .Value(result);
                            });
                    });
            });

        // TODO(akozhikhov): Add new format to avoid implicit use of newline symbol
        writer->Flush();
        Cout << ";" << Endl;
    }
}

void ParseAndValidate(
    const TString& exportConfig,
    THashSet<EObjectType>* searchedTypes,
    std::vector<TString>* searchedKeys,
    std::optional<int>* lowerIndex,
    std::optional<int>* upperIndex)
{
    auto config = ConvertTo<ExportArgumentsConfigPtr>(TYsonString(exportConfig));

    *lowerIndex = config->LowerIndex;
    *upperIndex = config->UpperIndex;

    if (config->PresetAttributes.has_value() ^ config->UserAttributes.empty()) {
        THROW_ERROR_EXCEPTION("Invalid export config: exactly one key 'attributes' or 'preset_attributes' must be defined");
    }

    if (config->PresetAttributes.has_value()) {
        if (config->PresetAttributes.value() == EPresetAttributes::Small) {
            *searchedKeys = preset_keys_small;
        } else if (config->PresetAttributes.value() == EPresetAttributes::Large) {
            *searchedKeys = preset_keys_large;
        }
    } else {
        *searchedKeys = std::move(config->UserAttributes);
    }

    if (lowerIndex->has_value() && lowerIndex->value() < 0) {
        THROW_ERROR_EXCEPTION("Invalid export config: 'lower_index' must be non-negative");
    }
    if (upperIndex->has_value() && upperIndex->value() <= 0) {
        THROW_ERROR_EXCEPTION("Invalid export config: 'upper_index' must be positive");
    }
    if (lowerIndex->has_value() && upperIndex->has_value() && lowerIndex->value() >= upperIndex->value()) {
        THROW_ERROR_EXCEPTION("Invalid export config: 'lower_index' must be less than 'upper_index'");
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

void ExportSnapshot (const TString& fileName, const TString& exportConfig, TBootstrap* bootstrap)
{
    THashSet<EObjectType> searchedTypes;
    std::vector<TString> searchedAttributes;
    /// We process [lowerIndex, upperIndex) interval, where lowerIndex and upperIndex denote indices of corresponding sorted keys of NodeMap_.
    std::optional<int> lowerIndex;
    std::optional<int> upperIndex;

    ParseAndValidate(exportConfig, &searchedTypes, &searchedAttributes, &lowerIndex, &upperIndex);

    bootstrap->TryLoadSnapshot(fileName, false);
    BIND(&DoExportSnapshot, bootstrap, lowerIndex, upperIndex, searchedAttributes, searchedTypes)
        .AsyncVia(bootstrap->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default))
        .Run()
        .Get()
        .ThrowOnError();
}

//////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
