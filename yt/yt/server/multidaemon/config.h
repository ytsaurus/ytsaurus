#pragma once

#include "public.h"

#include <yt/yt/library/server_program/config.h>

namespace NYT::NMultidaemon {

////////////////////////////////////////////////////////////////////////////////

struct TDaemonConfig
    : public NYTree::TYsonStruct
{
    std::string Type;
    NYTree::INodePtr Config;

    REGISTER_YSON_STRUCT(TDaemonConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDaemonConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMultidaemonProgramConfig
    : public TServerProgramConfig
{
    THashMap<std::string, TDaemonConfigPtr> Daemons;

    REGISTER_YSON_STRUCT(TMultidaemonProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMultidaemonProgramConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMultidaemon
