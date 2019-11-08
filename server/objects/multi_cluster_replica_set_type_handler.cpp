#include "multi_cluster_replica_set_type_handler.h"
#include "type_handler_detail.h"
#include "multi_cluster_replica_set.h"
#include "account.h"
#include "db_schema.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/access_control/access_control_manager.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TMultiClusterReplicaSetTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TMultiClusterReplicaSetTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::MultiClusterReplicaSet)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("account_id")
                    ->SetAttribute(TMultiClusterReplicaSet::TSpec::AccountSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetValidator<TMultiClusterReplicaSet>(std::bind(&TMultiClusterReplicaSetTypeHandler::ValidateAccount, this, _1, _2)),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TMultiClusterReplicaSet::TSpec::EtcSchema)
                    ->SetUpdatable()
            })
            ->SetExtensible()
            ->SetHistoryFilter<TMultiClusterReplicaSet>();

        StatusAttributeSchema_
            ->SetAttribute(TMultiClusterReplicaSet::StatusSchema)
            ->SetUpdatable()
            ->SetExtensible();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TMultiClusterReplicaSet>();
    }

    virtual const TDBField* GetIdField() override
    {
        return &MultiClusterReplicaSetsTable.Fields.Meta_Id;
    }

    virtual const TDBTable* GetTable() override
    {
        return &MultiClusterReplicaSetsTable;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TMultiClusterReplicaSet>(id, this, session);
    }

private:
    void ValidateAccount(TTransaction* /*transaction*/, TMultiClusterReplicaSet* replicaSet)
    {
        auto* account = replicaSet->Spec().Account().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(account, EAccessControlPermission::Use);
    }
};

std::unique_ptr<IObjectTypeHandler> CreateMultiClusterReplicaSetTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TMultiClusterReplicaSetTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

