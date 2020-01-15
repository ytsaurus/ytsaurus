#include "replica_set_type_handler.h"
#include "account.h"
#include "node_segment.h"
#include "type_handler_detail.h"
#include "replica_set.h"
#include "db_schema.h"
#include "pod_type_handler.h"
#include "config.h"

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
    TReplicaSetTypeHandler(NMaster::TBootstrap* bootstrap, TPodSpecValidationConfigPtr validationConfig)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::ReplicaSet)
        , PodSpecValidationConfig_(std::move(validationConfig))
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

                MakeAttributeSchema("node_segment_id")
                    ->SetAttribute(TReplicaSet::TSpec::NodeSegmentSchema)
                    ->SetUpdatable(),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TReplicaSet::TSpec::EtcSchema)
                    ->SetUpdatable()
                    ->SetValidator<TReplicaSet>(std::bind(&TReplicaSetTypeHandler::ValidateSpec, this, _1, _2)),
            })
            ->SetExtensible()
            ->EnableHistory();

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

    void ValidateSpec(TTransaction* transaction, TReplicaSet* replicaSet)
    {
        ValidateDeployPodSpecTemplate(Bootstrap_->GetAccessControlManager(), transaction, replicaSet->Spec().Etc().Load().pod_template_spec().spec(),
            PodSpecValidationConfig_);
    }

    void ValidateAccount(TTransaction* /*transaction*/, TReplicaSet* replicaSet)
    {
        auto* account = replicaSet->Spec().Account().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(account, EAccessControlPermission::Use);
    }

private:
    const TPodSpecValidationConfigPtr PodSpecValidationConfig_;
};

std::unique_ptr<IObjectTypeHandler> CreateReplicaSetTypeHandler(NMaster::TBootstrap* bootstrap, TPodSpecValidationConfigPtr validationConfig)
{
    return std::unique_ptr<IObjectTypeHandler>(new TReplicaSetTypeHandler(bootstrap, std::move(validationConfig)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

