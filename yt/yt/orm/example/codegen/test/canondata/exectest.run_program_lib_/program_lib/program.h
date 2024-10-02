// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include <yt/yt/orm/example/server/library/autogen/public.h>

#include <yt/yt/orm/example/server/library/autogen/config.h>

#include <yt/yt/orm/server/program/program.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

class TMasterProgram
    : public NYT::NOrm::NServer::NProgram::TMasterProgram<TMasterProgram, TMasterConfig>
{
public:
    TMasterProgram();

    static NYT::NOrm::NServer::NMaster::IBootstrap* StartBootstrap(
        TMasterConfigPtr config,
        NYT::NYTree::INodePtr configPatchNode);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
