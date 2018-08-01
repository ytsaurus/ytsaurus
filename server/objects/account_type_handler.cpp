#include "account_type_handler.h"
#include "type_handler_detail.h"
#include "account.h"
#include "pod_set.h"
#include "db_schema.h"
#include "transaction.h"

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NAccessControl;

////////////////////////////////////////////////////////////////////////////////

class TAccountTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TAccountTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Account)
    {
        StatusAttributeSchema_
            ->SetAttribute(TAccount::StatusSchema);

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("parent_id")
                    ->SetAttribute(TAccount::TSpec::ParentSchema)
                    ->SetUpdatable(),

                MakeFallbackAttributeSchema()
                    ->SetAttribute(TAccount::TSpec::OtherSchema)
                    ->SetUpdatable()
            });
    }

    virtual const TDBField* GetIdField() override
    {
        return &AccountsTable.Fields.Meta_Id;
    }

    virtual const TDBTable* GetTable() override
    {
        return &AccountsTable;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TAccount>(id, this, session);
    }

    virtual void BeforeObjectRemoved(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectRemoved(transaction, object);

        auto* account = object->As<TAccount>();
        auto* tmpAccount = transaction->GetAccount(TmpAccountId);
        for (auto* podSet : account->PodSets().Load()) {
            tmpAccount->PodSets().Add(podSet);
        }
    }

    virtual std::vector<EAccessControlPermission> GetDefaultPermissions() override
    {
        return {};
    }
};

std::unique_ptr<IObjectTypeHandler> CreateAccountTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TAccountTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

