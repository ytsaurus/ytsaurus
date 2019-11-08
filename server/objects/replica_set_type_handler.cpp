#include "replica_set_type_handler.h"
#include "account.h"
#include "type_handler_detail.h"
#include "replica_set.h"
#include "db_schema.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/access_control/access_control_manager.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TReplicaSetTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TReplicaSetTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::ReplicaSet)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("account_id")
                    ->SetAttribute(TReplicaSet::TSpec::AccountSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetMandatory()
                    ->SetValidator<TReplicaSet>(std::bind(&TReplicaSetTypeHandler::ValidateAccount, this, _1, _2)),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TReplicaSet::TSpec::EtcSchema)
                    ->SetUpdatable()
            })
            ->SetExtensible()
            ->SetHistoryFilter<TReplicaSet>();

        StatusAttributeSchema_
            ->SetAttribute(TReplicaSet::StatusSchema)
            ->SetUpdatable()
            ->SetExtensible();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TReplicaSet>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &ReplicaSetsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &ReplicaSetsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TReplicaSet(id, this, session));
    }

    void ValidateAccount(TTransaction* /*transaction*/, TReplicaSet* replicaSet)
    {
        auto* account = replicaSet->Spec().Account().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(account, EAccessControlPermission::Use);
    }
};

std::unique_ptr<IObjectTypeHandler> CreateReplicaSetTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TReplicaSetTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

