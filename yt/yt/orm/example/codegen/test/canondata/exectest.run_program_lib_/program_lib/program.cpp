// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "program.h"

#include <yt/yt/orm/example/server/library/bootstrap.h>

#include <yt/yt/orm/example/server/library/autogen/db_schema.h>

#include <yt/yt/orm/example/client/objects/autogen/init.h>

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

TMasterProgram::TMasterProgram()
    : NYT::NOrm::NServer::NProgram::TMasterProgram<TMasterProgram, TMasterConfig>(
        NYT::NOrm::NExample::NServer::NLibrary::DBVersion)
{
    NYT::NOrm::NExample::NClient::NObjects::EnsureGlobalRegistriesAreInitialized();
}

NYT::NOrm::NServer::NMaster::IBootstrap* TMasterProgram::StartBootstrap(
    TMasterConfigPtr config,
    NYT::NYTree::INodePtr configPatchNode)
{
    return NYT::NOrm::NExample::NServer::NLibrary::StartBootstrap(std::move(config), std::move(configPatchNode));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
