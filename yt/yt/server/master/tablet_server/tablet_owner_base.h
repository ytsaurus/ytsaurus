#pragma once

#include "public.h"
#include "tablet_statistics.h"

#include "config.h"

#include <yt/yt/server/master/chunk_server/chunk_owner_base.h>

#include <yt/yt/server/lib/misc/assert_sizeof.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletOwnerBase
    : public NChunkServer::TChunkOwnerBase
{
private:
    using TTabletStateIndexedVector = TEnumIndexedArray<
        NTabletClient::ETabletState,
        int,
        NTabletClient::MinValidTabletState,
        NTabletClient::MaxValidTabletState>;
    using TTabletList = std::vector<TTabletBase*>;

    struct TTabletOwnerAttributes
    {
        TTabletList Tablets;
        TTabletStateIndexedVector TabletCountByState;
        TTabletStateIndexedVector TabletCountByExpectedState;

        i64 TabletMasterMemoryUsage = 0;
        int TabletErrorCount = 0;

        TString MountPath;

        TTabletResources ExternalTabletResourceUsage;

        NTabletClient::ETabletState ActualTabletState = NTabletClient::ETabletState::Unmounted;
        NTabletClient::ETabletState ExpectedTabletState = NTabletClient::ETabletState::Unmounted;
        NTransactionClient::TTransactionId LastMountTransactionId;
        NTransactionClient::TTransactionId PrimaryLastMountTransactionId;
        NTransactionClient::TTransactionId CurrentMountTransactionId;

        TTabletStatisticsAggregate TabletStatistics;

        NTabletClient::EInMemoryMode InMemoryMode = NTabletClient::EInMemoryMode::None;

        NHydra::TRevision SettingsUpdateRevision = NHydra::NullRevision;
        int RemountNeededTabletCount = 0;

        void Save(NCellMaster::TSaveContext& context) const;
        void Load(NCellMaster::TLoadContext& context);

        void CopyFrom(const TTabletOwnerAttributes* other);
        void BeginCopy(NCypressServer::TBeginCopyContext* context) const;
        void EndCopy(NCypressServer::TEndCopyContext* context);
    };

public:
    DEFINE_BYREF_RW_PROPERTY(TTabletCellBundlePtr, TabletCellBundle);

    DECLARE_EXTRA_PROPERTY_HOLDER(TTabletOwnerAttributes, TabletOwnerAttributes);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, InMemoryMode);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(TabletOwnerAttributes, TabletCountByState);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(TabletOwnerAttributes, TabletCountByExpectedState);
    DEFINE_BYREF_RW_EXTRA_PROPERTY(TabletOwnerAttributes, Tablets);

    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, TabletMasterMemoryUsage);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, TabletErrorCount);

    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, MountPath);

    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, ExternalTabletResourceUsage);

    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, ActualTabletState);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, ExpectedTabletState);

    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, LastMountTransactionId);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, PrimaryLastMountTransactionId);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, CurrentMountTransactionId);

    DEFINE_BYVAL_EXTRA_AGGREGATE_PROPERTY(TabletOwnerAttributes, TabletStatistics);

    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, SettingsUpdateRevision);
    DEFINE_BYVAL_RW_EXTRA_PROPERTY(TabletOwnerAttributes, RemountNeededTabletCount);

    using TChunkOwnerBase::TChunkOwnerBase;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    TTabletOwnerBase* GetTrunkNode();
    const TTabletOwnerBase* GetTrunkNode() const;

    NSecurityServer::TDetailedMasterMemory GetDetailedMasterMemoryUsage() const override;
    void RecomputeTabletMasterMemoryUsage();

    // COMPAT(gritukan): Remove after RecomputeTabletErrorCount.
    void RecomputeTabletErrorCount();

    TTabletResources GetTabletResourceUsage() const override;

    NTabletClient::ETabletState GetTabletState() const;

    NTabletClient::ETabletState ComputeActualTabletState() const;

    void UpdateExpectedTabletState(NTabletClient::ETabletState state);

    void ValidateNoCurrentMountTransaction(TStringBuf message) const;
    void ValidateTabletStateFixed(TStringBuf message) const;
    void ValidateAllTabletsFrozenOrUnmounted(TStringBuf message) const;
    void ValidateAllTabletsUnmounted(TStringBuf message) const;

    virtual void ValidateMount() const;
    virtual void ValidateUnmount() const;
    virtual void ValidateRemount() const;
    virtual void ValidateFreeze() const;
    virtual void ValidateUnfreeze() const;
    virtual void ValidateReshard(
        const NCellMaster::TBootstrap* bootstrap,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const std::vector<i64>& trimmedRowCounts) const;

    void LockCurrentMountTransaction(NTransactionClient::TTransactionId transactionId);
    void UnlockCurrentMountTransaction(NTransactionClient::TTransactionId transactionId);

    void OnRemountNeeded();

private:
    void ValidateExpectedTabletState(TStringBuf message, bool allowFrozen) const;
};

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TTabletOwnerBase, 656);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
