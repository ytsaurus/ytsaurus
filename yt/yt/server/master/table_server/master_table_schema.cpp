#include "master_table_schema.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/helpers.h>
#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/table_server/table_manager.h>

namespace NYT::NTableServer {

using namespace NSecurityServer;
using namespace NTableClient;
using namespace NYson;

///////////////////////////////////////////////////////////////////////////////

TMasterTableSchema::TMasterTableSchema(TMasterTableSchemaId id, TTableSchemaToObjectMapIterator it)
    : TBase(id)
{
    SetTableSchemaToObjectMapIterator(it);
}

void TMasterTableSchema::Save(NCellMaster::TSaveContext& context) const
{
    TBase::Save(context);

    using NYT::Save;

    Save(context, *TableSchema_);
}

void TMasterTableSchema::Load(NCellMaster::TLoadContext& context)
{
    TBase::Load(context);

    using NYT::Load;

    auto tableSchema = Load<TTableSchema>(context);

    if (IsObjectAlive(this)) {
        const auto& tableManager = context.GetBootstrap()->GetTableManager();
        SetTableSchemaToObjectMapIterator(tableManager->RegisterSchema(this, std::move(tableSchema)));
    } else {
        TableSchema_ = New<TTableSchema>(std::move(tableSchema));
    }
}

const NTableClient::TTableSchemaPtr& TMasterTableSchema::AsTableSchema() const
{
    YT_ASSERT(IsObjectAlive(this));

    return TableSchema_;
}

const TFuture<TYsonString>& TMasterTableSchema::AsYsonAsync() const
{
    if (!MemoizedYson_) {
        MemoizedYson_ = BIND([schema = AsTableSchema()] {
            return NYTree::ConvertToYsonString(schema);
        })
        .AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker())
        .Run();
    }
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
    return NYTree::ConvertToYsonString(*AsTableSchema());
}

bool TMasterTableSchema::RefBy(TAccount* account)
{
    YT_VERIFY(IsObjectAlive(account));

    return ++ReferencingAccounts_[account] == 1;
}

bool TMasterTableSchema::UnrefBy(TAccount* account)
{
    YT_VERIFY(IsObjectAlive(account));

    auto it = ReferencingAccounts_.find(account);
    YT_VERIFY(it != ReferencingAccounts_.end());
    if (--it->second == 0) {
        ReferencingAccounts_.erase(it);
        return true;
    } else {
        YT_VERIFY(it->second > 0);
        return false;
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
    } else {
        auto [it, inserted] = ChargedMasterMemoryUsage_.insert({account, usage});
        if (!inserted) {
            it->second = usage;
        }
    }
}

TMasterTableSchema::TTableSchemaToObjectMapIterator TMasterTableSchema::GetTableSchemaToObjectMapIterator() const
{
    return TableSchemaToObjectMapIterator_;
}

void TMasterTableSchema::SetTableSchemaToObjectMapIterator(TTableSchemaToObjectMapIterator it)
{
    TableSchemaToObjectMapIterator_ = it;
    TableSchema_ = it->first;
}

void TMasterTableSchema::ResetTableSchemaToObjectMapIterator()
{
    TableSchemaToObjectMapIterator_ = {};
    // NB: Retain TableSchema_ for possible future snapshot serialization.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
