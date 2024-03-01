#include "table_settings.h"
#include "config.h"

#include <yt/yt/library/re2/re2.h>

namespace NYT::NTabletNode {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

NYTree::IMapNodePtr DefaultMapNodeCtor()
{
    return GetEphemeralNodeFactory()->CreateMap();
}

////////////////////////////////////////////////////////////////////////////////

void TTableIOConfigPatch::Register(TRegistrar registrar)
{
    registrar.Parameter("store_reader_config", &TThis::StoreReaderConfig)
        .DefaultCtor(&DefaultMapNodeCtor)
        .ResetOnLoad();
    registrar.Parameter("hunk_reader_config", &TThis::HunkReaderConfig)
        .DefaultCtor(&DefaultMapNodeCtor)
        .ResetOnLoad();
    registrar.Parameter("store_writer_config", &TThis::StoreWriterConfig)
        .DefaultCtor(&DefaultMapNodeCtor)
        .ResetOnLoad();
    registrar.Parameter("hunk_writer_config", &TThis::HunkWriterConfig)
        .DefaultCtor(&DefaultMapNodeCtor)
        .ResetOnLoad();

    // Unrecognized parameters are undesired at the top level. We keep them and
    // validate their absence during postprocessing.
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::Keep);

    registrar.Postprocessor([&] (TTableIOConfigPatch* config) {
        ConvertTo<TTabletStoreReaderConfigPtr>(config->StoreReaderConfig);
        ConvertTo<TTabletHunkReaderConfigPtr>(config->HunkReaderConfig);
        ConvertTo<TTabletStoreWriterConfigPtr>(config->StoreWriterConfig);
        ConvertTo<TTabletHunkWriterConfigPtr>(config->HunkWriterConfig);

        auto unrecognized = config->GetLocalUnrecognized();
        if (unrecognized && unrecognized->GetChildCount() > 0) {
            THROW_ERROR_EXCEPTION("Unrecognized fields in table io config patch: %v",
                MakeFormattableView(
                    unrecognized->GetKeys(),
                    TDefaultFormatter{}));
        }
    });
}

bool TTableIOConfigPatch::IsEqual(const TTableIOConfigPatchPtr& other) const
{
    return
        AreNodesEqual(StoreReaderConfig, other->StoreReaderConfig) &&
        AreNodesEqual(HunkReaderConfig, other->HunkReaderConfig) &&
        AreNodesEqual(StoreWriterConfig, other->StoreWriterConfig) &&
        AreNodesEqual(HunkWriterConfig, other->HunkWriterConfig);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void ValidateNoForbiddenKeysInPatch(const NYTree::IMapNodePtr& keys, TStringBuf patchKind)
{
    static constexpr std::array ExperimentModificationForbidden{
        "tablet_cell_bundle",
        "in_memory_mode",
        "profiling_mode",
        "profiling_tag",
        "enable_dynamic_store_read",
        "enable_consistent_chunk_replica_placement",
        "enable_detailed_profiling",
    };
    static_assert(ExperimentModificationForbidden.size() == 7,
        "Consider promoting master reign");

    for (auto forbiddenKey : ExperimentModificationForbidden) {
        if (keys->FindChild(forbiddenKey)) {
            THROW_ERROR_EXCEPTION("Forbidden to change field %Qlv in experiments, fix your %v",
                forbiddenKey,
                patchKind);
        }
    }
}

} // namespace

void TTableConfigPatch::Register(TRegistrar registrar)
{
    registrar.Parameter("mount_config_template_patch", &TThis::MountConfigTemplatePatch)
        .DefaultCtor(&DefaultMapNodeCtor)
        .ResetOnLoad();
    registrar.Parameter("mount_config_patch", &TThis::MountConfigPatch)
        .DefaultCtor(&DefaultMapNodeCtor)
        .ResetOnLoad();
    registrar.Parameter("io_config_patch", &TThis::IOConfigPatch)
        .DefaultNew();

    registrar.Postprocessor([&] (TTableConfigPatch* config) {
        ValidateNoForbiddenKeysInPatch(config->MountConfigTemplatePatch, "template patch");
        ValidateNoForbiddenKeysInPatch(config->MountConfigPatch, "patch");

        if (config->MountConfigTemplatePatch &&
            config->MountConfigTemplatePatch->GetChildCount() > 0)
        {
            ConvertTo<TCustomTableMountConfigPtr>(config->MountConfigTemplatePatch);
        }

        if (config->MountConfigPatch &&
            config->MountConfigPatch->GetChildCount() > 0)
        {
            ConvertTo<TCustomTableMountConfigPtr>(config->MountConfigPatch);
        }
    });
}

bool TTableConfigPatch::IsEqual(const TTableConfigPatchPtr& other) const
{
    return
        AreNodesEqual(MountConfigTemplatePatch, other->MountConfigTemplatePatch) &&
        AreNodesEqual(MountConfigPatch, other->MountConfigPatch) &&
        IOConfigPatch->IsEqual(other->IOConfigPatch);
}

////////////////////////////////////////////////////////////////////////////////

void TTableConfigExperiment::Register(TRegistrar registrar)
{
    registrar.Parameter("salt", &TThis::Salt)
        .Default();
    registrar.Parameter("fraction", &TThis::Fraction)
        .InRange(0, 1);

    registrar.Parameter("patch", &TThis::Patch)
        .DefaultNew();

    registrar.Parameter("path_re", &TThis::PathRe)
        .Default();
    registrar.Parameter("tablet_cell_bundle", &TThis::TabletCellBundle)
        .Default();
    registrar.Parameter("in_memory_mode", &TThis::InMemoryMode)
        .Default();
    registrar.Parameter("sorted", &TThis::Sorted)
        .Default();
    registrar.Parameter("replicated", &TThis::Replicated)
        .Default();
    registrar.Parameter("auto_apply", &TThis::AutoApply)
        .Default(false);

    registrar.Postprocessor([&] (TTableConfigExperiment* experiment) {
        double numerator = experiment->Fraction * experiment->FractionDenominator;
        experiment->FractionNumerator = std::llround(numerator);

        constexpr static double AbsoluteError = 1e-6;
        if (std::abs(numerator - experiment->FractionNumerator) > AbsoluteError) {
            THROW_ERROR_EXCEPTION("Fraction must be a multiple of 1/%v",
                experiment->FractionDenominator);
        }
    });
}

bool TTableConfigExperiment::Matches(const TTableDescriptor& descriptor, bool oldBehaviorCompat) const
{
    if (Sorted.has_value() && *Sorted != descriptor.Sorted) {
        return false;
    }

    if (Replicated.has_value() && *Replicated != descriptor.Replicated) {
        return false;
    }

    if (PathRe && !NRe2::TRe2::FullMatch(NRe2::StringPiece(descriptor.TablePath), *PathRe)) {
        return false;
    }

    // COMPAT(dave11ar): Remove oldBehaviorCompat from Matches and DropIrrelevantExperiments.
    if (oldBehaviorCompat) {
        if (TabletCellBundle && TabletCellBundle->pattern() != descriptor.TabletCellBundle) {
            return false;
        }
    } else {
        if (TabletCellBundle && !NRe2::TRe2::FullMatch(NRe2::StringPiece(descriptor.TabletCellBundle), *TabletCellBundle)) {
            return false;
        }

        if (InMemoryMode && !NRe2::TRe2::FullMatch(NRe2::StringPiece(FormatEnum(descriptor.InMemoryMode)), *InMemoryMode)) {
            return false;
        }
    }

    auto hash = ComputeHash(descriptor.TableId);
    HashCombine(hash, ComputeHash(Salt));

    return static_cast<int>(hash % FractionDenominator) < FractionNumerator;
}

////////////////////////////////////////////////////////////////////////////////

void TClusterTableConfigPatchSet::Register(TRegistrar registrar)
{
    registrar.Parameter("table_config_experiments", &TThis::TableConfigExperiments)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TTableSettings TTableSettings::CreateNew()
{
    return {
        .MountConfig = New<TTableMountConfig>(),
        .StoreReaderConfig = New<TTabletStoreReaderConfig>(),
        .HunkReaderConfig = New<TTabletHunkReaderConfig>(),
        .StoreWriterConfig = New<TTabletStoreWriterConfig>(),
        .StoreWriterOptions = New<TTabletStoreWriterOptions>(),
        .HunkWriterConfig = New<TTabletHunkWriterConfig>(),
        .HunkWriterOptions = New<TTabletHunkWriterOptions>()
    };
}

////////////////////////////////////////////////////////////////////////////////

void TRawTableSettings::CreateNewProvidedConfigs()
{
    Provided.StoreReaderConfig = New<TTabletStoreReaderConfig>();
    Provided.HunkReaderConfig = New<TTabletHunkReaderConfig>();
    Provided.StoreWriterConfig = New<TTabletStoreWriterConfig>();
    Provided.StoreWriterOptions = New<TTabletStoreWriterOptions>();
    Provided.HunkWriterConfig = New<TTabletHunkWriterConfig>();
    Provided.HunkWriterOptions = New<TTabletHunkWriterOptions>();
}

void TRawTableSettings::DropIrrelevantExperiments(
    const TTableConfigExperiment::TTableDescriptor& descriptor,
    bool oldBehaviorCompat)
{
    for (auto it = Experiments.begin(); it != Experiments.end(); ) {
        if (it->second->Matches(descriptor, oldBehaviorCompat)) {
            ++it;
        } else {
            it = Experiments.erase(it);
        }
    }
}

std::pair<TTableMountConfigPtr, IMapNodePtr> DeserializeTableMountConfig(
    const IMapNodePtr& configNode,
    const IMapNodePtr& extraAttributes,
    std::vector<TError>* errors)
{
    try {
        if (!extraAttributes) {
            return {ConvertTo<TTableMountConfigPtr>(configNode), configNode};
        }

        auto mountConfigMap = ConvertTo<IMapNodePtr>(configNode);
        auto patchedMountConfigMap = PatchNode(mountConfigMap, extraAttributes);

        try {
            return {
                ConvertTo<TTableMountConfigPtr>(patchedMountConfigMap),
                patchedMountConfigMap->AsMap()
            };
        } catch (const std::exception& ex) {
            if (errors) {
                errors->push_back(TError(
                    "Error deserializing tablet mount config "
                    "with extra attributes patch")
                        << ex);
            }
            return {
                ConvertTo<TTableMountConfigPtr>(mountConfigMap),
                mountConfigMap->AsMap()
            };
        }
    } catch (const std::exception& ex) {
        if (errors) {
            errors->push_back(TError(
                "Error deserializing tablet mount config")
                    << ex);
        }
        return {
            New<TTableMountConfig>(),
            GetEphemeralNodeFactory()->CreateMap()
        };
    }
}

std::pair<TTableSettings, TTableConfigPatchPtr> TryApplySinglePatch(
    TTableSettings settings,
    const IMapNodePtr& initialMountConfigNode,
    const TTableConfigPatchPtr& existingPatch,
    const TTableConfigPatchPtr& newPatch)
{
    auto resultingPatch = New<TTableConfigPatch>();

    auto patchNodeLazy = [] (const auto& lhs, const auto& rhs) {
        return rhs->GetChildCount() > 0
            ? PatchNode(lhs, rhs)->AsMap()
            : lhs;
    };

    auto applyPatch = [&] (auto&& getter) {
        getter(resultingPatch) = patchNodeLazy(getter(existingPatch), getter(newPatch));
    };

    applyPatch([] (auto&& x) -> auto& { return x->MountConfigTemplatePatch; });
    applyPatch([] (auto&& x) -> auto& { return x->MountConfigPatch; });
    applyPatch([] (auto&& x) -> auto& { return x->IOConfigPatch->StoreReaderConfig; });
    applyPatch([] (auto&& x) -> auto& { return x->IOConfigPatch->HunkReaderConfig; });
    applyPatch([] (auto&& x) -> auto& { return x->IOConfigPatch->StoreWriterConfig; });
    applyPatch([] (auto&& x) -> auto& { return x->IOConfigPatch->HunkWriterConfig; });

    // New patch is built, now try applying it to the provided settings.
    if (resultingPatch->MountConfigTemplatePatch->GetChildCount() > 0 ||
        resultingPatch->MountConfigPatch->GetChildCount() > 0)
    {
        try {
            auto node = PatchNode(
                resultingPatch->MountConfigTemplatePatch,
                initialMountConfigNode);
            node = PatchNode(node, resultingPatch->MountConfigPatch);
            settings.MountConfig = ConvertTo<TTableMountConfigPtr>(node);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to apply table mount config patch")
                << ex;
        }
    }

    auto convertIOPatch = [&] (TStringBuf configType, auto&& getter) {
        if (getter(*resultingPatch->IOConfigPatch)->GetChildCount() == 0) {
            return;
        }

        try {
            auto node = PatchNode(
                ConvertToNode(getter(settings)),
                getter(*resultingPatch->IOConfigPatch));
            getter(settings) = ConvertTo<std::decay_t<decltype(getter(settings))>>(node);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to apply %v patch", configType)
                << ex;
        }
    };

    convertIOPatch("store_reader", [] (auto&& x) -> auto& { return x.StoreReaderConfig; });
    convertIOPatch("hunk_reader", [] (auto&& x) -> auto& { return x.HunkReaderConfig; });
    convertIOPatch("store_writer", [] (auto&& x) -> auto& { return x.StoreWriterConfig; });
    convertIOPatch("hunk_writer", [] (auto&& x) -> auto& { return x.HunkWriterConfig; });

    return {std::move(settings), std::move(resultingPatch)};
}

TTableSettings TRawTableSettings::BuildEffectiveSettings(
    std::vector<TError>* errors,
    std::vector<TString>* unappliedExperimentNames) const
{
    TTableSettings initialSettings{
        .StoreReaderConfig = Provided.StoreReaderConfig,
        .HunkReaderConfig = Provided.HunkReaderConfig,
        .StoreWriterConfig = Provided.StoreWriterConfig,
        .StoreWriterOptions = Provided.StoreWriterOptions,
        .HunkWriterConfig = Provided.HunkWriterConfig,
        .HunkWriterOptions = Provided.HunkWriterOptions,
    };

    // All configs have already been deserialized except mount config.
    // We check that it can be deserialized and that extra attributes are applied.
    IMapNodePtr initialMountConfigNode;
    std::tie(initialSettings.MountConfig, initialMountConfigNode) =
        DeserializeTableMountConfig(
            Provided.MountConfigNode,
            Provided.ExtraMountConfig,
            errors);

    auto resultingSettings = initialSettings;
    auto resultingPatch = New<TTableConfigPatch>();

    // Now apply global patch and experiments one by one, sandwiching mount config
    // between patch and template patch and applying patches over IO configs.
    try {
        std::tie(resultingSettings, resultingPatch) =
            TryApplySinglePatch(
                initialSettings,
                initialMountConfigNode,
                resultingPatch,
                GlobalPatch);
    } catch (const std::exception& ex) {
        if (errors) {
            errors->push_back(TError("Failed to apply global table config patch")
                << ex);
        }
    }

    for (const auto& [name, experiment] : Experiments) {
        try {
            std::tie(resultingSettings, resultingPatch) =
                TryApplySinglePatch(
                    initialSettings,
                    initialMountConfigNode,
                    resultingPatch,
                    experiment->Patch);
        } catch (const std::exception& ex) {
            if (errors) {
                errors->push_back(TError("Failed to apply table config patch experiment %Qlv",
                    name)
                    << ex);
            }
            if (unappliedExperimentNames) {
                unappliedExperimentNames->push_back(name);
            }
        }
    }

    return resultingSettings;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
