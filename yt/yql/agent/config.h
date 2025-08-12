#pragma once

#include "private.h"

#include <yt/yql/plugin/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/net/config.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NYqlAgent {

using NYqlPlugin::TYqlPluginConfig;

////////////////////////////////////////////////////////////////////////////////

struct TYqlAgentConfig
    : public TYqlPluginConfig
{
    TDuration TokenExpirationTimeout;
    TDuration RefreshTokenPeriod;
    int IssueTokenAttempts;

    int YqlThreadCount;
    std::optional<std::string> MaxSupportedYqlVersion;

    REGISTER_YSON_STRUCT(TYqlAgentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentConfig)

////////////////////////////////////////////////////////////////////////////////

struct TYqlAgentDynamicConfig
    : public NYTree::TYsonStruct
{
    int MaxSimultaneousQueries;
    TDuration StateCheckPeriod;

    //! Fields from NYql::TGatewaysConfig with snake case keys.
    NYTree::INodePtr GatewaysConfig;

    REGISTER_YSON_STRUCT(TYqlAgentDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TYqlAgentServerConfig
    : public NServer::TNativeServerBootstrapConfig
    , public TServerProgramConfig
{
    TYqlAgentConfigPtr YqlAgent;

    bool AbortOnUnrecognizedOptions;

    //! User for native client.
    TString User;

    //! The path of directory for orchids.
    NYPath::TYPath Root;

    NYTree::IMapNodePtr CypressAnnotations;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    REGISTER_YSON_STRUCT(TYqlAgentServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentServerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TYqlAgentServerDynamicConfig
    : public TSingletonsDynamicConfig
{
    TYqlAgentDynamicConfigPtr YqlAgent;

    REGISTER_YSON_STRUCT(TYqlAgentServerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentServerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
