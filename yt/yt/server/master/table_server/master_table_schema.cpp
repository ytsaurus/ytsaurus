#include "master_table_schema.h"

#include "private.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/helpers.h>
#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/table_server/table_manager.h>

#include "private.h"

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NTableClient;
using namespace NYson;

static const auto& Logger = TableServerLogger;

///////////////////////////////////////////////////////////////////////////////

TMasterTableSchema::TMasterTableSchema(TMasterTableSchemaId id, TNativeTableSchemaToObjectMapIterator it)
    : TBase(id)
{
    SetNativeTableSchemaToObjectMapIterator(it);
}

TMasterTableSchema::TMasterTableSchema(TMasterTableSchemaId id, TTableSchemaPtr schema)
    : TBase(id)
    , TableSchema_(std::move(schema))
{
    SetForeign();
}

void TMasterTableSchema::Save(NCellMaster::TSaveContext& context) const
{
    TBase::Save(context);

    using NYT::Save;

    Save(context, *TableSchema_);
    Save(context, CellTagToExportCount_);
    Save(context, ReferencingAccounts_);
}

void TMasterTableSchema::Load(NCellMaster::TLoadContext& context)
{
    TBase::Load(context);

    using NYT::Load;

    auto tableSchema = Load<TTableSchema>(context);

    // COMPAT(h0pless)
    if (context.GetVersion() < EMasterReign::ExportMasterTableSchemas) {
        if (IsObjectAlive(this)) {
            const auto& tableManager = context.GetBootstrap()->GetTableManager();
            SetNativeTableSchemaToObjectMapIterator(tableManager->RegisterNativeSchema(this, std::move(tableSchema)));
        } else {
            TableSchema_ = New<TTableSchema>(std::move(tableSchema));
        }
    } else {
        if (IsObjectAlive(this)) {
            const auto& tableManager = context.GetBootstrap()->GetTableManager();

            if (IsNative()) {
                SetNativeTableSchemaToObjectMapIterator(tableManager->RegisterNativeSchema(this, std::move(tableSchema)));
            } else {
                // Imported schemas require no registration because reverse
                // index for imported schemas is not necessary.
                TableSchema_ = New<TTableSchema>(std::move(tableSchema));
            }
        } else {
            TableSchema_ = New<TTableSchema>(std::move(tableSchema));
        }

        if (context.GetVersion() < EMasterReign::RecomputeMasterTableSchemaRefCounters) {
            if (Load<bool>(context)) {
                CellTagToExportCount_ = Load<TCellTagToExportRefcount>(context);
            }
        } else {
            Load(context, CellTagToExportCount_);
        }
    }

    if (context.GetVersion() >= EMasterReign::AddChunkSchemas) {
        Load(context, ReferencingAccounts_);
    }
}

const NTableClient::TTableSchemaPtr& TMasterTableSchema::AsTableSchema(bool crashOnZombie) const
{
    YT_VERIFY(IsObjectAlive(this) || !crashOnZombie);

    return TableSchema_;
}

const TFuture<TYsonString>& TMasterTableSchema::AsYsonAsync() const
{
    // NB: Can be called from local read threads.
    auto readerGuard = ReaderGuard(MemoizedYsonLock_);
    if (MemoizedYson_) {
        return MemoizedYson_;
    }

    readerGuard.Release();
    auto writerGuard = WriterGuard(MemoizedYsonLock_);
    if (MemoizedYson_) {
        return MemoizedYson_;
    }

    MemoizedYson_ = BIND([schema = AsTableSchema()] {
        return NYson::ConvertToYsonString(schema);
    })
        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
        .Run();

    return MemoizedYson_;
}

TYsonString TMasterTableSchema::AsYsonSync() const
{
    // It's quite likely that this schema has already been serialized. And even
    // if it hasn't, it's wise to start the serialization.
    const auto& asyncYson = AsYsonAsync();
    if (auto optionalYsonOrError = asyncYson.TryGet()) {
        return optionalYsonOrError->ValueOrThrow();
    }

    // There's no escape - serialize it right here and now.
    return NYson::ConvertToYsonString(*AsTableSchema());
}

bool TMasterTableSchema::RefBy(TAccount* account, int delta)
{
    YT_VERIFY(delta > 0);

    auto [it, inserted] = ReferencingAccounts_.emplace(std::move(TAccountPtr(account)), delta);
    if (!inserted) {
        it->second += delta;
    }
    return inserted;
}

bool TMasterTableSchema::UnrefBy(TAccount* account, int delta)
{
    YT_VERIFY(delta > 0);

    auto it = ReferencingAccounts_.find(account);
    if (it == ReferencingAccounts_.end()) {
        YT_LOG_ALERT("Attempting to unref schema by account that holds no recorded refs (SchemaId: %v, Account: %v)",
            Id_,
            account->GetName());
        return false;
    }

    it->second -= delta;
    if (it->second == 0) {
        ReferencingAccounts_.erase(it);
        return true;
    }

    YT_LOG_ALERT_UNLESS(it->second > 0,
        "Negative account ref count on schema detected (SchemaId: %v, Account: %v)",
        Id_,
        account->GetName());
    return false;
}

bool TMasterTableSchema::IsExported(TCellTag cellTag) const
{
    auto it = CellTagToExportCount_.find(cellTag);
    if (it == CellTagToExportCount_.end()) {
        return false;
    }

    YT_VERIFY(it->second > 0);
    return true;
}

void TMasterTableSchema::AlertIfNonEmptyExportCount()
{
    if (!CellTagToExportCount_.empty()) {
        YT_LOG_ALERT("Table schema being destroyed has non-empty export count (SchemaId: %v, ExportCount: %v)",
            GetId(),
            CellTagToExportCount_.size());
    }
}

i64 TMasterTableSchema::GetMasterMemoryUsage(TAccount* account) const
{
    return ReferencingAccounts_.contains(account) ? AsTableSchema()->GetMemoryUsage() : 0;
}

i64 TMasterTableSchema::GetChargedMasterMemoryUsage(TAccount* account) const
{
    auto it = ChargedMasterMemoryUsage_.find(account);
    return it == ChargedMasterMemoryUsage_.end()
        ? i64(0)
        : it->second;
}

void TMasterTableSchema::SetChargedMasterMemoryUsage(TAccount* account, i64 usage)
{
    if (usage == 0) {
        ChargedMasterMemoryUsage_.erase(account);
        return;
    }

    auto [it, inserted] = ChargedMasterMemoryUsage_.emplace(account, usage);
    if (!inserted) {
        it->second = usage;
    }
}

void TMasterTableSchema::SetId(TMasterTableSchemaId id)
{
    Id_ = id;
}

void TMasterTableSchema::ResetExportRefCounters()
{
    YT_LOG_DEBUG("Resetting export ref counters for schema (SchemaId: %v)",
        GetId());
    CellTagToExportCount_.clear();
}

TMasterTableSchema::TNativeTableSchemaToObjectMapIterator TMasterTableSchema::GetNativeTableSchemaToObjectMapIterator() const
{
    YT_VERIFY(IsNative());
    return NativeTableSchemaToObjectMapIterator_;
}

void TMasterTableSchema::SetNativeTableSchemaToObjectMapIterator(TNativeTableSchemaToObjectMapIterator it)
{
    YT_VERIFY(IsNative());
    NativeTableSchemaToObjectMapIterator_ = it;
    TableSchema_ = it->first;
}

void TMasterTableSchema::ResetNativeTableSchemaToObjectMapIterator()
{
    NativeTableSchemaToObjectMapIterator_ = {};
    // NB: Retain TableSchema_ for possible future snapshot serialization.
}

void TMasterTableSchema::ExportRef(TCellTag cellTag)
{
    YT_VERIFY(cellTag != NotReplicatedCellTagSentinel);

    auto [it, inserted] = CellTagToExportCount_.emplace(cellTag, 1);
    if (!inserted) {
        YT_VERIFY(it->second > 0);
        ++it->second;
    }

    YT_LOG_DEBUG("Schema export counter incremented (SchemaId: %v, CellTag: %v, ExportCounter: %v)",
        GetId(),
        cellTag,
        it->second);
}

// NB: UnexportRef should be only called on native cells.
void TMasterTableSchema::UnexportRef(TCellTag cellTag, int decreaseBy)
{
    YT_VERIFY(cellTag != NotReplicatedCellTagSentinel);
    YT_VERIFY(CellTagToExportCount_);

    auto it = GetIteratorOrCrash(CellTagToExportCount_, cellTag);
    YT_VERIFY(it->second >= decreaseBy);

    it->second -= decreaseBy;

    YT_LOG_DEBUG("Schema export counter decremented (SchemaId: %v, CellTag: %v, ExportCounter: %v, DecreaseBy: %v)",
        GetId(),
        cellTag,
        it->second,
        decreaseBy);

    if (it->second != 0) {
        return;
    }

    CellTagToExportCount_.erase(it);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
