#include "account_type_handler.h"
#include "type_handler_detail.h"
#include "account.h"
#include "stage.h"
#include "pod.h"
#include "pod_set.h"
#include "replica_set.h"
#include "multi_cluster_replica_set.h"
#include "db_schema.h"
#include "transaction.h"

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

////////////////////////////////////////////////////////////////////////////////

class TAccountTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TAccountTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Account)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        StatusAttributeSchema_
            ->SetAttribute(TAccount::StatusSchema);

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("parent_id")
                    ->SetAttribute(TAccount::TSpec::ParentSchema)
                    ->SetUpdatable(),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TAccount::TSpec::EtcSchema)
                    ->SetUpdatable()
            });
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TAccount>();
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

    virtual void BeforeObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* account = object->As<TAccount>();
        account->Status()->mutable_resource_usage();
        account->Status()->mutable_immediate_resource_usage();
    }

    virtual void BeforeObjectRemoved(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectRemoved(transaction, object);

        auto* account = object->As<TAccount>();
        auto* tmpAccount = transaction->GetAccount(TmpAccountId);
        for (auto* podSet : account->PodSets().Load()) {
            tmpAccount->PodSets().Add(podSet);
        }
    }

protected:
    virtual std::vector<EAccessControlPermission> GetDefaultPermissions() override
    {
        return {};
    }

    virtual bool IsObjectNameSupported() const override
    {
        return true;
    }
};

std::unique_ptr<IObjectTypeHandler> CreateAccountTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TAccountTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

