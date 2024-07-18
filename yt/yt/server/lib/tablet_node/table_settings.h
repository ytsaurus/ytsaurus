#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/re2/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Collection of patches for table IO configs. All patches are applied after
//! user-provided config.
class TTableIOConfigPatch
    : public NYTree::TYsonStruct
{
public:
    NYTree::IMapNodePtr StoreReaderConfig;
    NYTree::IMapNodePtr HunkReaderConfig;
    NYTree::IMapNodePtr StoreWriterConfig;
    NYTree::IMapNodePtr HunkWriterConfig;

    bool IsEqual(const TTableIOConfigPatchPtr& other) const;

    REGISTER_YSON_STRUCT(TTableIOConfigPatch);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableIOConfigPatch)

///////////////////////////////////////////////////////////////////////////////

//! Collection of patches for table configs:
//! - mount config template patch (applied before user-provided config)
//! - mount config patch (applied after user-provided config)
//! - IO patches (applied after user-provided config)
class TTableConfigPatch
    : public virtual NYTree::TYsonStruct
{
public:
    NYTree::IMapNodePtr MountConfigTemplatePatch;
    NYTree::IMapNodePtr MountConfigPatch;
    TTableIOConfigPatchPtr IOConfigPatch;

    bool IsEqual(const TTableConfigPatchPtr& other) const;

    REGISTER_YSON_STRUCT(TTableConfigPatch);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableConfigPatch)

///////////////////////////////////////////////////////////////////////////////

//! Description of a table config experiment. An experiment is made of, roughly speaking,
//! a patch ("which options are changed"), selectors ("which tables belong to the domain")
//! and a fraction ("which portion of tables of the domain should be affected").
class TTableConfigExperiment
    : public NYTree::TYsonStruct
{
public:
    //! Patch to apply.
    TTableConfigPatchPtr Patch;

    //! Following selectors specify which tables are in the domain of the experiment.

    //! If not null, take only tables whose paths match the RE.
    NRe2::TRe2Ptr PathRe;
    //! If not null, take only tables of the specified tablet cell bundle.
    NRe2::TRe2Ptr TabletCellBundle;
    //! If not null, take only tables wih specified memory mode.
    NRe2::TRe2Ptr InMemoryMode;
    //! If not null, take only sorted/non-sorted tables respectively.
    std::optional<bool> Sorted;
    //! If not null, take only replicated/non-replicated tables respectively.
    std::optional<bool> Replicated;

    //! Arbitrary string necessary to make experiments independent. Used to seed a random
    //! generator before tossing a coin to check if a certain table is affected by the
    //! experiment.
    TString Salt;

    //! Fraction of tables of the domain that are affected by the experiment.
    //! Stored as a rational number to make deterministic choices and avoid precision errors.
    static constexpr i64 FractionDenominator = 1000;
    i64 FractionNumerator;

    //! If true, all affected tables will re-apply the experiment whenever it is updated
    //! in the master config. Otherwise all patches induced by the experiment will start
    //! taking place only after explicit remount.
    bool AutoApply;

    REGISTER_YSON_STRUCT(TTableConfigExperiment);

    static void Register(TRegistrar registrar);

public:
    struct TTableDescriptor
    {
        const NTableClient::TTableId TableId;
        const TString TablePath;
        const TString TabletCellBundle;
        const NTabletClient::EInMemoryMode InMemoryMode;
        const bool Sorted;
        const bool Replicated;
    };

    //! Returns true iff the experiment affects a table denoted by |descriptor|. That is,
    //! the table belongs to the working set and a certain random number is less than |Fraction|.
    //! Random number deterministically depends on |descriptor.TableId| and |Salt|.
    bool Matches(const TTableDescriptor& descriptor, bool oldBehaviorCompat = false) const;

private:
    double Fraction;
};

DEFINE_REFCOUNTED_TYPE(TTableConfigExperiment)

////////////////////////////////////////////////////////////////////////////////

//! Mixin that serves as an entry point for all cluster-wide table config patches.
//! Resulting structure is as follows:
//! {
//!     mount_config_patch = {};
//!     mount_config_template_patch = {};
//!     io_config_patch = {
//!         store_reader_config = {};
//!         ...
//!     };
//!     table_config_experiments = {};
//! };
class TClusterTableConfigPatchSet
    : public TTableConfigPatch
{
public:
    std::map<TString, TTableConfigExperimentPtr> TableConfigExperiments;

    REGISTER_YSON_STRUCT(TClusterTableConfigPatchSet);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TClusterTableConfigPatchSet)

////////////////////////////////////////////////////////////////////////////////

//! Collection of ready-to-use table configs (mount config and IO configs).
//! This is the only class the user should interact with after all patches are applied.
struct TTableSettings
{
    TTableMountConfigPtr MountConfig;
    TTabletStoreReaderConfigPtr StoreReaderConfig;
    TTabletHunkReaderConfigPtr HunkReaderConfig;
    TTabletStoreWriterConfigPtr StoreWriterConfig;
    TTabletStoreWriterOptionsPtr StoreWriterOptions;
    TTabletHunkWriterConfigPtr HunkWriterConfig;
    TTabletHunkWriterOptionsPtr HunkWriterOptions;

    //! Initializes all fields with corresponding New<...>().
    static TTableSettings CreateNew();
};

////////////////////////////////////////////////////////////////////////////////

//! Detailed description of all provided configs and patches for a certain table.
//! The purpose of this class is to allow adding and removing patches on the fly,
//! knowing which options were explicitly provided by the user and which came from
//! patches and experiments.
struct TRawTableSettings
{
    //! Explicitly provided configs.
    //! The difference between mount config and IO configs is dictated by the way
    //! they are dealt with at master. Mount config is being sent as a node, allowing
    //! for extra template patches ("under user config") being applied later.
    //! IO configs are fully materialized before being sent to the tablet cell,
    //! so only regular patches ("over user config") can be applied.
    struct
    {
        //! Recognized (by master) portion of the @mount_config attribute.
        NYTree::IMapNodePtr MountConfigNode;

        //! Unrecognized (by master) portion of the same attribute. Unrecognized
        //! options are still sent to the tablet cell since the node may have
        //! more recent binary and be able to apply those options.
        NYTree::IMapNodePtr ExtraMountConfig;

        //! The following configs are results of merging templates from
        //! "store_chunk_reader" (and similar) options of the tablet manager dynamic
        //! config and patches from @chunk_reader (and similar) attributes of the table.
        TTabletStoreReaderConfigPtr StoreReaderConfig;
        TTabletHunkReaderConfigPtr HunkReaderConfig;
        TTabletStoreWriterConfigPtr StoreWriterConfig;
        TTabletStoreWriterOptionsPtr StoreWriterOptions;
        TTabletHunkWriterConfigPtr HunkWriterConfig;
        TTabletHunkWriterOptionsPtr HunkWriterOptions;
    } Provided;

    TTableConfigPatchPtr GlobalPatch;
    // NB: Use tree map to ensure deterministic order and allow efficient
    // set difference and set intersection.
    std::map<TString, TTableConfigExperimentPtr> Experiments;

    //! Initializes all provided IO configs with corresponding New<...>().
    void CreateNewProvidedConfigs();

    //! Removes from the |Experiments| map all experiments which do not match |descriptor|.
    void DropIrrelevantExperiments(
        const TTableConfigExperiment::TTableDescriptor& descriptor,
        bool oldBehaviorCompat);

    //! Combines all patches, experiments and provided settings. Each patch is either
    //! applied completely or not applied at all. If a patch cannot be applied, related
    //! error is appended to |errors|.
    TTableSettings BuildEffectiveSettings(
        std::vector<TError>* errors,
        std::vector<TString>* malformedExperimentNames) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
