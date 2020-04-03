#include "multi_cluster_replica_set_type_handler.h"
#include "type_handler_detail.h"
#include "multi_cluster_replica_set.h"
#include "account.h"
#include "node_segment.h"
#include "db_schema.h"
#include "pod_type_handler.h"
#include "config.h"
#include "validation_helpers.h"

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
    TMultiClusterReplicaSetTypeHandler(NMaster::TBootstrap* bootstrap, TPodSpecValidationConfigPtr validationConfig)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::MultiClusterReplicaSet)
        , PodSpecValidationConfig_(std::move(validationConfig))
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

                MakeAttributeSchema("node_segment_id")
                    ->SetAttribute(TMultiClusterReplicaSet::TSpec::NodeSegmentSchema)
                    ->SetUpdatable(),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TMultiClusterReplicaSet::TSpec::EtcSchema)
                    ->SetUpdatable()
                    ->SetValidator<TMultiClusterReplicaSet>(std::bind(&TMultiClusterReplicaSetTypeHandler::ValidateSpec, this, _1, _2))
            })
            ->SetExtensible()
            ->EnableHistory();

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
    const TPodSpecValidationConfigPtr PodSpecValidationConfig_;

    void ValidateSpec(TTransaction* transaction, TMultiClusterReplicaSet* replicaSet)
    {
        ValidateDeployPodSpecTemplate(
            Bootstrap_->GetAccessControlManager(),
            transaction,
            replicaSet->Spec().Etc().LoadOld().pod_template_spec().spec(),
            replicaSet->Spec().Etc().Load().pod_template_spec().spec(),
            PodSpecValidationConfig_);
    }

    void ValidateAccount(TTransaction* /*transaction*/, TMultiClusterReplicaSet* replicaSet)
    {
        ValidateUsePermissionIfChanged(replicaSet->Spec().Account(), Bootstrap_->GetAccessControlManager());
    }
};

std::unique_ptr<IObjectTypeHandler> CreateMultiClusterReplicaSetTypeHandler(NMaster::TBootstrap* bootstrap, TPodSpecValidationConfigPtr validationConfig)
{
    return std::unique_ptr<IObjectTypeHandler>(new TMultiClusterReplicaSetTypeHandler(bootstrap, std::move(validationConfig)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

