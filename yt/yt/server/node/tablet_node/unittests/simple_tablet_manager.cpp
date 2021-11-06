#include "simple_tablet_manager.h"

#include <yt/yt/server/node/tablet_node/structured_logger.h>
#include <yt/yt/server/node/tablet_node/store_manager.h>
#include <yt/yt/server/node/tablet_node/sorted_store_manager.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/client/table_client/name_table.h>

namespace NYT::NTabletNode {

using namespace NHydra;
using namespace NTableClient;
using namespace NCypressClient;
using namespace NTabletClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

TSimpleTabletManager::TSimpleTabletManager(
    TTransactionManagerPtr transactionManager,
    ISimpleHydraManagerPtr hydraManager,
    TCompositeAutomatonPtr automaton,
    IInvokerPtr automatonInvoker)
    : TTabletAutomatonPart(
        NullCellId,
        std::move(hydraManager),
        std::move(automaton),
        std::move(automatonInvoker))
    , TransactionManager_(std::move(transactionManager))
{
    RegisterLoader(
        "SimpleTabletManager.Values",
        BIND(&TSimpleTabletManager::LoadValues, Unretained(this)));
    RegisterLoader(
        "SimpleTabletManager.Async",
        BIND(&TSimpleTabletManager::LoadAsync, Unretained(this)));
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

    Tablet_ = std::make_unique<TTablet>(
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
        0);

    Tablet_->SetStructuredLogger(CreateMockPerTabletStructuredLogger(Tablet_.get()));

    InitializeStoreManager();

    StoreManager_->StartEpoch(nullptr);
    StoreManager_->Mount({}, {}, true, NProto::TMountHint{});
}

void TSimpleTabletManager::InitializeStoreManager()
{
    StoreManager_ = New<TSortedStoreManager>(
        New<TTabletManagerConfig>(),
        Tablet_.get(),
        &TabletContext_);

    Tablet_->SetStoreManager(StoreManager_);
}

TTablet* TSimpleTabletManager::GetTabletOrThrow(TTabletId id)
{
    YT_VERIFY(id == NullTabletId);

    return Tablet_.get();
}

void TSimpleTabletManager::OnTabletUnlocked(TTablet* /*tablet*/)
{ }

void TSimpleTabletManager::OnTabletRowUnlocked(TTablet* /*tablet*/)
{ }

TTablet* TSimpleTabletManager::FindTablet(const TTabletId& id) const
{
    YT_VERIFY(id == NullTabletId);

    return Tablet_.get();
}

TTransactionManagerPtr TSimpleTabletManager::GetTransactionManager() const
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

bool TSimpleTabletManager::ValidateAndDiscardRowRef(const TSortedDynamicRowRef& /*rowRef*/)
{
    return true;
}

void TSimpleTabletManager::AdvanceReplicatedTrimmedRowCount(TTablet* /*tablet*/, TTransaction* /*transaction*/)
{ }

TCellId TSimpleTabletManager::GetCellId() const
{
    return NullCellId;
}

TTablet* TSimpleTabletManager::Tablet()
{
    return Tablet_.get();
}

void TSimpleTabletManager::LoadValues(TLoadContext& context)
{
    using NYT::Load;

    Tablet_ = std::make_unique<TTablet>(NullTabletId, &TabletContext_);

    Load(context, *Tablet_);

    Tablet_->SetStructuredLogger(CreateMockPerTabletStructuredLogger(Tablet_.get()));

    InitializeStoreManager();
}

void TSimpleTabletManager::LoadAsync(TLoadContext& context)
{
    Tablet_->AsyncLoad(context);
}

void TSimpleTabletManager::SaveValues(TSaveContext& context)
{
    using NYT::Save;

    Save(context, *Tablet_);
}

TCallback<void(TSaveContext&)> TSimpleTabletManager::SaveAsync()
{
    return Tablet_->AsyncSave();
}

void TSimpleTabletManager::Clear()
{
    TCompositeAutomatonPart::Clear();

    Tablet_.reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
