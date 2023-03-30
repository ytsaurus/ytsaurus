#include "simple_tablet_manager.h"

#include <yt/yt/server/node/tablet_node/structured_logger.h>
#include <yt/yt/server/node/tablet_node/store_manager.h>
#include <yt/yt/server/node/tablet_node/sorted_store_manager.h>
#include <yt/yt/server/node/tablet_node/ordered_store_manager.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/client/table_client/name_table.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NTabletNode {

using namespace NHydra;
using namespace NTableClient;
using namespace NCypressClient;
using namespace NTabletClient;
using namespace NQueryClient;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TSimpleTabletManager::TSimpleTabletManager(
    ITransactionManagerPtr transactionManager,
    ISimpleHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker)
    : TTabletAutomatonPart(
        NullCellId,
        std::move(hydraManager),
        std::move(automaton),
        automatonInvoker)
    , AutomatonInvoker_(std::move(automatonInvoker))
    , TransactionManager_(std::move(transactionManager))
    , TabletMap_(TTabletMapTraits(this))
    , TabletContext_(this)
{
    RegisterLoader(
        "SimpleTabletManager.Keys",
        BIND(&TSimpleTabletManager::LoadKeys, Unretained(this)));
    RegisterLoader(
        "SimpleTabletManager.Values",
        BIND(&TSimpleTabletManager::LoadValues, Unretained(this)));
    RegisterLoader(
        "SimpleTabletManager.Async",
        BIND(&TSimpleTabletManager::LoadAsync, Unretained(this)));

    RegisterSaver(
        ESyncSerializationPriority::Keys,
        "SimpleTabletManager.Keys",
        BIND(&TSimpleTabletManager::SaveKeys, Unretained(this)));
    RegisterSaver(
        ESyncSerializationPriority::Values,
        "SimpleTabletManager.Values",
        BIND(&TSimpleTabletManager::SaveValues, Unretained(this)));
    RegisterSaver(
        EAsyncSerializationPriority::Default,
        "SimpleTabletManager.Async",
        BIND(&TSimpleTabletManager::SaveAsync, Unretained(this)));
}

void TSimpleTabletManager::InitializeTablet(TTabletOptions options)
{
    bool sorted = options.Schema->IsSorted();

    auto nameTable = TNameTable::FromSchema(*options.Schema);

    auto tablet = std::make_unique<TTablet>(
        NullTabletId,
        TTableSettings::CreateNew(),
        0,
        NullObjectId,
        "ut",
        &TabletContext_,
        /*schemaId*/ NullObjectId,
        options.Schema,
        sorted ? MinKey() : TLegacyOwningKey(),
        sorted ? MaxKey() : TLegacyOwningKey(),
        options.Atomicity,
        options.CommitOrdering,
        TTableReplicaId(),
        /*retainedTimestamp*/ NullTimestamp,
        /*cumulativeDataWeight*/ 0);

    TRawTableSettings rawSettings;
    rawSettings.CreateNewProvidedConfigs();
    rawSettings.Provided.MountConfigNode = CreateEphemeralNodeFactory()
        ->CreateMap();
    rawSettings.GlobalPatch = New<TTableConfigPatch>();
    tablet->RawSettings() = std::move(rawSettings);

    tablet->SetStructuredLogger(CreateMockPerTabletStructuredLogger(tablet.get()));
    WaitFor(BIND([&, tablet = std::move(tablet)] () mutable {
        TabletMap_.Insert(NullTabletId, std::move(tablet));
    })
        .AsyncVia(AutomatonInvoker_)
        .Run())
        .ThrowOnError();

    InitializeStoreManager(sorted);

    StoreManager_->StartEpoch(nullptr);
    StoreManager_->Mount({}, {}, true, NProto::TMountHint{});
}

void TSimpleTabletManager::InitializeStoreManager(bool sorted)
{
    auto* tablet = GetTablet();
    if (sorted) {
        StoreManager_ = New<TSortedStoreManager>(
            New<TTabletManagerConfig>(),
            tablet,
            &TabletContext_);
    } else {
        StoreManager_ = New<TOrderedStoreManager>(
            New<TTabletManagerConfig>(),
            tablet,
            &TabletContext_);
    }

    tablet->SetStoreManager(StoreManager_);
}

TTablet* TSimpleTabletManager::GetTabletOrThrow(TTabletId id)
{
    YT_VERIFY(id == NullTabletId);

    return TabletMap_.Get(NullTabletId);
}

i64 TSimpleTabletManager::LockTablet(TTablet* /*tablet*/, ETabletLockType /*lockType*/)
{
    return 0;
}

i64 TSimpleTabletManager::UnlockTablet(TTablet* /*tablet*/, ETabletLockType /*lockType*/)
{
    return 0;
}

void TSimpleTabletManager::OnTabletRowUnlocked(TTablet* /*tablet*/)
{ }

TTablet* TSimpleTabletManager::FindTablet(const TTabletId& id) const
{
    YT_VERIFY(id == NullTabletId);

    return TabletMap_.Get(id);
}

TTablet* TSimpleTabletManager::GetTablet(const TTabletId& id) const
{
    auto* tablet = FindTablet(id);
    YT_VERIFY(tablet);

    return tablet;
}

const NHydra::TReadOnlyEntityMap<TTablet>& TSimpleTabletManager::Tablets() const
{
    return TabletMap_;
}

ITransactionManagerPtr TSimpleTabletManager::GetTransactionManager() const
{
    return TransactionManager_;
}

TDynamicTabletCellOptionsPtr TSimpleTabletManager::GetDynamicOptions() const
{
    return DynamicOptions_;
}

TTabletManagerConfigPtr TSimpleTabletManager::GetConfig() const
{
    return Config_;
}

void TSimpleTabletManager::ValidateMemoryLimit(const std::optional<TString>& /*poolTag*/)
{ }

TTimestamp TSimpleTabletManager::GetLatestTimestamp() const
{
    // TODO(max42): use the same value with tablet slot.
    return TTimestamp();
}

bool TSimpleTabletManager::ValidateRowRef(const TSortedDynamicRowRef& /*rowRef*/)
{
    return true;
}

bool TSimpleTabletManager::ValidateAndDiscardRowRef(const TSortedDynamicRowRef& /*rowRef*/)
{
    return true;
}

void TSimpleTabletManager::AdvanceReplicatedTrimmedRowCount(TTablet* /*tablet*/, TTransaction* /*transaction*/)
{ }

const IBackupManagerPtr& TSimpleTabletManager::GetBackupManager() const
{
    const static IBackupManagerPtr NullBackupManager = nullptr;
    return NullBackupManager;
}

TCellId TSimpleTabletManager::GetCellId() const
{
    return NullCellId;
}

TTabletNodeDynamicConfigPtr TSimpleTabletManager::GetDynamicConfig() const
{
    auto config = New<TTabletNodeDynamicConfig>();
    YT_VERIFY(config->TabletCellWriteManager);
    YT_VERIFY(!config->TabletCellWriteManager->WriteFailureProbability);
    return config;
}

ISimpleHydraManagerPtr TSimpleTabletManager::GetHydraManager() const
{
    return HydraManager_;
}

TTablet* TSimpleTabletManager::GetTablet()
{
    return TabletMap_.Get(NullTabletId);
}

void TSimpleTabletManager::LoadKeys(TLoadContext& context)
{
    using NYT::Load;

    TabletMap_.LoadKeys(context);
}

void TSimpleTabletManager::LoadValues(TLoadContext& context)
{
    using NYT::Load;

    TabletMap_.LoadValues(context);

    auto* tablet = GetTablet();

    tablet->SetStructuredLogger(CreateMockPerTabletStructuredLogger(tablet));

    InitializeStoreManager(tablet->GetTableSchema()->IsSorted());
}

void TSimpleTabletManager::LoadAsync(TLoadContext& context)
{
    GetTablet()->AsyncLoad(context);
}

void TSimpleTabletManager::SaveKeys(TSaveContext& context)
{
    using NYT::Save;

    TabletMap_.SaveKeys(context);
}

void TSimpleTabletManager::SaveValues(TSaveContext& context)
{
    using NYT::Save;

    TabletMap_.SaveValues(context);
}

TCallback<void(TSaveContext&)> TSimpleTabletManager::SaveAsync()
{
    return GetTablet()->AsyncSave();
}

void TSimpleTabletManager::Clear()
{
    TCompositeAutomatonPart::Clear();

    WaitFor(BIND([&] {
        TabletMap_.Clear();
    })
        .AsyncVia(AutomatonInvoker_)
        .Run())
        .ThrowOnError();
}

void TSimpleTabletManager::OnAfterSnapshotLoaded()
{
    TCompositeAutomatonPart::OnAfterSnapshotLoaded();

    for (auto [tabletId, tablet] : TabletMap_) {
        tablet->OnAfterSnapshotLoaded();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
