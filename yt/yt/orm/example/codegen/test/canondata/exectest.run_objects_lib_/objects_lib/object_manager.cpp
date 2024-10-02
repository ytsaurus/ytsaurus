// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "object_manager.h"

#include "db_schema.h"
#include "type_handlers.h"

#include <yt/yt/orm/server/master/bootstrap.h>

#include <yt/yt/orm/server/objects/config.h>

#include <yt/yt/orm/server/objects/object_manager.h>

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

namespace {

DECLARE_REFCOUNTED_CLASS(TDataModelObjectManager)

class TDataModelObjectManager
    : public NYT::NOrm::NServer::NObjects::TObjectManager
{
public:
    TDataModelObjectManager(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
        : TObjectManager(bootstrap, std::move(config), Tables)
    { }

    bool AreHistoryEnabledAttributePathsEnabled() const override
    {
        return false;
    }

protected:
    void RegisterTypeHandlers() override
    {
        auto* bootstrap = GetBootstrap();
        auto config = GetConfig();
        RegisterTypeHandler(CreateAuthorTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateBookTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateBufferedTimestampIdTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateCatTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateEditorTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateEmployerTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateExecutorTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateGenreTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateGroupTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateHitchhikerTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateIllustratorTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateIndexedIncrementIdTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateInterceptorTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateManualIdTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateMotherShipTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateMultipolicyIdTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateNestedColumnsTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateNexusTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateNirvanaDMProcessInstanceTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreatePublisherTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateRandomIdTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateSchemaTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateSemaphoreTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateSemaphoreSetTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateTimestampIdTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateTypographerTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateUserTypeHandler(bootstrap, config));
        RegisterTypeHandler(CreateWatchLogConsumerTypeHandler(bootstrap, config));
    }
};

DEFINE_REFCOUNTED_TYPE(TDataModelObjectManager)

} // namespace

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NServer::NObjects::TObjectManagerPtr CreateObjectManager(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return New<TDataModelObjectManager>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
