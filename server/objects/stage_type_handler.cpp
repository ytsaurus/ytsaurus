#include "stage_type_handler.h"
#include "account.h"
#include "db_schema.h"
#include "stage.h"
#include "type_handler_detail.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/access_control/access_control_manager.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TStageTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TStageTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::Stage)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("account_id")
                    ->SetAttribute(TStage::TSpec::AccountSchema
                        .SetNullable(false))
                    ->SetUpdatable()
                    ->SetValidator<TStage>(std::bind(&TStageTypeHandler::ValidateAccount, this, _1, _2)),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TStage::TSpec::EtcSchema)
                    ->SetUpdatable()
            })
            ->SetExtensible();

        StatusAttributeSchema_
            ->SetAttribute(TStage::StatusSchema)
            ->SetUpdatable()
            ->SetExtensible();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TStage>();
    }

    virtual const TDBField* GetIdField() override
    {
        return &StagesTable.Fields.Meta_Id;
    }

    virtual const TDBTable* GetTable() override
    {
        return &StagesTable;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TStage>(id, this, session);
    }

private:
    void ValidateAccount(TTransaction* /*transaction*/, TStage* Stage)
    {
        auto* account = Stage->Spec().Account().Load();
        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        accessControlManager->ValidatePermission(account, EAccessControlPermission::Use);
    }
};

std::unique_ptr<IObjectTypeHandler> CreateStageTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TStageTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

