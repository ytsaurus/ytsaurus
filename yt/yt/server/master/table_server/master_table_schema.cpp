#include "master_table_schema.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NTableServer {

using namespace NSecurityServer;
using namespace NTableClient;
using namespace NYson;

///////////////////////////////////////////////////////////////////////////////

TMasterTableSchema::TMasterTableSchema(TMasterTableSchemaId id, TTableSchemaToObjectMapIterator it)
    : TBase(id)
    , TableSchemaToObjectMapIterator_(it)
{ }

TMasterTableSchema::TMasterTableSchema(TMasterTableSchemaId id)
    :TBase(id)
{ }

void TMasterTableSchema::Save(NCellMaster::TSaveContext& context) const
{
    TBase::Save(context);

    using NYT::Save;

    Save(context, AsTableSchema());
}

void TMasterTableSchema::Load(NCellMaster::TLoadContext& context)
{
    TBase::Load(context);

    using NYT::Load;

    auto tableSchema = Load<TTableSchema>(context);

    const auto& tableManager = context.GetBootstrap()->GetTableManager();
    TableSchemaToObjectMapIterator_ = tableManager->InitializeSchema(this, tableSchema);
}

const NTableClient::TTableSchema& TMasterTableSchema::AsTableSchema() const
{
    return TableSchemaToObjectMapIterator_->first;
}

const TFuture<TYsonString>& TMasterTableSchema::AsYsonAsync(const NObjectServer::TObjectManagerPtr& objectManager)
{
    YT_ASSERT(!IsDestroyed());
    objectManager->EphemeralRefObject(this);
    auto automatonInvoker = GetCurrentInvoker();

    if (!MemoizedYson_) {
        const auto& rpcInvoker = NRpc::TDispatcher::Get()->GetHeavyInvoker();
        MemoizedYson_ = BIND([objectManager, schema = this, automatonInvoker = std::move(automatonInvoker)] {
            auto unrefSchema = [&] {
                automatonInvoker->Invoke(BIND([schema, objectManager = std::move(objectManager)] {
                    objectManager->EphemeralUnrefObject(schema);
                }));
            };

            if (!IsObjectAlive(schema)) {
                auto schemaId = schema->GetId();
                unrefSchema();
                THROW_ERROR_EXCEPTION("Schema object is already destroyed")
                    << TErrorAttribute("object_id", schemaId);
            }

            auto result = NYTree::ConvertToYsonString(schema->AsTableSchema());
            unrefSchema();
            return result;
        })
        .AsyncVia(rpcInvoker)
        .Run();
    }
    return MemoizedYson_;
}

TYsonString TMasterTableSchema::AsYsonSync(const NObjectServer::TObjectManagerPtr& objectManager)
{
    // It's quite likely that this schema has already been serialized. And even
    // if it hasn't, it's wise to start the serialization.
    const auto& asyncYson = AsYsonAsync(objectManager);
    if (asyncYson.IsSet() && asyncYson.Get().IsOK()) {
        return asyncYson.Get().Value();
    }

    // There's no escape - serialize it right here and now.
    return NYTree::ConvertToYsonString(AsTableSchema());
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
    return ReferencingAccounts_.contains(account) ? AsTableSchema().GetMemoryUsage() : 0;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
