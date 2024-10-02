// AUTOMATICALLY GENERATED. DO NOT EDIT!
#include "object_detail.h"
#include "type_handler_impls.h"
#include "type_handlers.h"

#include <yt/yt/orm/server/objects/group_type_handler_detail.h>
#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/server/objects/schema_type_handler_detail.h>
#include <yt/yt/orm/server/objects/semaphore_detail.h>
#include <yt/yt/orm/server/objects/semaphore_set_type_handler_detail.h>
#include <yt/yt/orm/server/objects/semaphore_type_handler_detail.h>
#include <yt/yt/orm/server/objects/subject_type_handler_detail.h>
#include <yt/yt/orm/server/objects/user_type_handler_detail.h>
#include <yt/yt/orm/server/objects/watch_log_consumer_type_handler_detail.h>
#include <yt/yt/orm/example/plugins/server/library/custom_base_type_handler.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler>
CreateCatTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler>
CreateEditorTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler>
CreateEmployerTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler>
CreateGenreTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler>
CreateManualIdTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler>
CreateMotherShipTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

std::unique_ptr<NYT::NOrm::NServer::NObjects::IObjectTypeHandler>
CreatePublisherTypeHandler(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NPlugins

namespace NYT::NOrm::NExample::NServer::NLibrary {

using NYT::NOrm::NServer::NMaster::IBootstrap;
using NYT::NOrm::NServer::NObjects::IObjectTypeHandler;

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IObjectTypeHandler> CreateAuthorTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TAuthorTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateBookTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TBookTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateBufferedTimestampIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TBufferedTimestampIdTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateCatTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NExample::NServer::NPlugins::CreateCatTypeHandler(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateEditorTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NExample::NServer::NPlugins::CreateEditorTypeHandler(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateEmployerTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NExample::NServer::NPlugins::CreateEmployerTypeHandler(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateExecutorTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TExecutorTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateGenreTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NExample::NServer::NPlugins::CreateGenreTypeHandler(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateHitchhikerTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<THitchhikerTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateIllustratorTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TIllustratorTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateIndexedIncrementIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TIndexedIncrementIdTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateInterceptorTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TInterceptorTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateManualIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NExample::NServer::NPlugins::CreateManualIdTypeHandler(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateMotherShipTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NExample::NServer::NPlugins::CreateMotherShipTypeHandler(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateMultipolicyIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TMultipolicyIdTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateNestedColumnsTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TNestedColumnsTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateNexusTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TNexusTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateNirvanaDMProcessInstanceTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TNirvanaDMProcessInstanceTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreatePublisherTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NExample::NServer::NPlugins::CreatePublisherTypeHandler(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateRandomIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TRandomIdTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateTimestampIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TTimestampIdTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateTypographerTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return std::make_unique<TTypographerTypeHandler>(bootstrap, config);

}

std::unique_ptr<IObjectTypeHandler> CreateGroupTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NServer::NObjects::CreateGroupTypeHandler<TGroup, TGroupTypeHandler>(
        bootstrap,
        std::move(config));
}

std::unique_ptr<IObjectTypeHandler> CreateUserTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NServer::NObjects::CreateUserTypeHandler<TUser, TUserTypeHandler>(bootstrap, std::move(config));
}

std::unique_ptr<IObjectTypeHandler> CreateSchemaTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NServer::NObjects::CreateSchemaTypeHandler<TSchemaTypeHandler>(
        bootstrap,
        std::move(config));
}

std::unique_ptr<IObjectTypeHandler> CreateWatchLogConsumerTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NServer::NObjects::CreateWatchLogConsumerTypeHandler<
        TWatchLogConsumer,
        TWatchLogConsumerTypeHandler>(bootstrap, std::move(config));
}

std::unique_ptr<IObjectTypeHandler> CreateSemaphoreTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NServer::NObjects::CreateSemaphoreTypeHandler<
        TFinalSemaphore,
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphore,
        TSemaphoreTypeHandler>(bootstrap, std::move(config));
}

std::unique_ptr<IObjectTypeHandler> CreateSemaphoreSetTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
{
    return NYT::NOrm::NServer::NObjects::CreateSemaphoreSetTypeHandler<
        TFinalSemaphoreSet,
        NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSet,
        TSemaphoreSetTypeHandler>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
