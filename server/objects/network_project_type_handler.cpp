#include "network_project_type_handler.h"

#include "db_schema.h"
#include "network_project.h"
#include "type_handler_detail.h"

namespace NYP::NServer::NObjects {

using std::placeholders::_1;

////////////////////////////////////////////////////////////////////////////////

class TNetworkProjectTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TNetworkProjectTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::NetworkProject)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("project_id")
                    ->SetAttribute(TNetworkProject::TSpec::ProjectIdSchema)
                    ->SetMandatory()
                    ->SetUpdatable()
                    ->EnableHistory(THistoryEnabledAttributeSchema()
                        .SetValueFilter<TNetworkProject>(std::bind(&TNetworkProjectTypeHandler::SpecProjectIdHistoryFilter, this, _1)))
            });

        StatusAttributeSchema_
            ->SetComposite();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TNetworkProject>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &NetworkProjectsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &NetworkProjectsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& /*parentId*/,
        ISession* session) override
    {
        return std::make_unique<TNetworkProject>(id, this, session);
    }

private:
    bool SpecProjectIdHistoryFilter(TNetworkProject* networkProject)
    {
        return networkProject->Spec().ProjectId().LoadOld() != networkProject->Spec().ProjectId().Load();
    }
};

std::unique_ptr<IObjectTypeHandler> CreateNetworkProjectTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TNetworkProjectTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

