#pragma once

#include "tablet_context_mock.h"

#include <yt/yt/server/node/tablet_node/automaton.h>
#include <yt/yt/server/node/tablet_node/tablet_write_manager.h>
#include <yt/yt/server/node/tablet_node/tablet.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/tablet_client/config.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TTabletOptions
{
    NTableClient::TTableSchemaPtr Schema = New<TTableSchema>(std::vector{
        TColumnSchema(TColumnSchema("k", EValueType::Int64).SetSortOrder(NTableClient::ESortOrder::Ascending)),
        TColumnSchema(TColumnSchema("v", EValueType::Int64)),
    });
    NTransactionClient::EAtomicity Atomicity = NTransactionClient::EAtomicity::Full;
    NTransactionClient::ECommitOrdering CommitOrdering = NTransactionClient::ECommitOrdering::Weak;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleTabletManager
    : public ITabletWriteManagerHost
    , public TTabletAutomatonPart
{
public:
    TSimpleTabletManager(
        TTransactionManagerPtr transactionManager,
        NHydra::ISimpleHydraManagerPtr hydraManager,
        NHydra::TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker);
    void InitializeTablet(TTabletOptions options);
    void InitializeStoreManager(bool sorted);

    // ITabletWriteManagerHost

    void OnTabletUnlocked(TTablet* tablet) override;
    void OnTabletRowUnlocked(TTablet* tablet) override;
    TTablet* GetTabletOrThrow(TTabletId id) override;
    TTablet* FindTablet(const TTabletId& id) const override;
    const NHydra::TReadOnlyEntityMap<TTablet>& Tablets() const override;
    TTransactionManagerPtr GetTransactionManager() const override;
    NTabletClient::TDynamicTabletCellOptionsPtr GetDynamicOptions() const override;
    TTabletManagerConfigPtr GetConfig() const override;
    void ValidateMemoryLimit(const std::optional<TString>& /*poolTag*/) override;
    TTimestamp GetLatestTimestamp() const override;
    bool ValidateAndDiscardRowRef(const TSortedDynamicRowRef& /*rowRef*/) override;
    void AdvanceReplicatedTrimmedRowCount(TTablet* /*tablet*/, TTransaction* /*transaction*/) override;
    TCellId GetCellId() const override;

    TTablet* GetTablet();

private:
    class TTabletMapTraits
    {
    public:
        explicit TTabletMapTraits(TSimpleTabletManager* owner)
            : Owner_(owner)
        { }

        std::unique_ptr<TTablet> Create(TTabletId id) const
        {
            return std::make_unique<TTablet>(id, &Owner_->TabletContext_);
        }

    private:
        TSimpleTabletManager* const Owner_;
    };

    // This invoker helps to deal with TabletMap_ thread affinity verifications.
    const IInvokerPtr AutomatonInvoker_;
    const TTransactionManagerPtr TransactionManager_;

    NTabletClient::TDynamicTabletCellOptionsPtr DynamicOptions_ = New<NTabletClient::TDynamicTabletCellOptions>();
    TTabletManagerConfigPtr Config_ = New<TTabletManagerConfig>();

    NHydra::TEntityMap<TTablet, TTabletMapTraits> TabletMap_;

    IStoreManagerPtr StoreManager_;
    TTabletContextMock TabletContext_;

    void LoadKeys(TLoadContext& context);
    void LoadValues(TLoadContext& context);
    void LoadAsync(TLoadContext& context);
    void SaveKeys(TSaveContext& context);
    void SaveValues(TSaveContext& context);
    TCallback<void(TSaveContext&)> SaveAsync();

    void Clear() override;
};

DECLARE_REFCOUNTED_CLASS(TSimpleTabletManager)
DEFINE_REFCOUNTED_TYPE(TSimpleTabletManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
