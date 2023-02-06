#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

class TYqlEmbeddedConfig
    : public NYTree::TYsonStruct
{
public:
    TString MRJobBinary;

    TString YTToken;

    REGISTER_YSON_STRUCT(TYqlEmbeddedConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlEmbeddedConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentConfig
    : public TYqlEmbeddedConfig
{
public:
    //! Used to create channels to other queue agents.
    NBus::TBusConfigPtr BusClient;

    std::vector<TString> AdditionalClusters;

    int YqlThreadCount;

    REGISTER_YSON_STRUCT(TYqlAgentConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TYqlAgentDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentServerConfig
    : public TNativeServerConfig
{
public:
    TYqlAgentConfigPtr YqlAgent;

    bool AbortOnUnrecognizedOptions;

    //! User for native client.
    TString User;

    //! The path of directory for orchids.
    NYPath::TYPath Root;

    NYTree::IMapNodePtr CypressAnnotations;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    REGISTER_YSON_STRUCT(TYqlAgentServerConfig)

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentServerConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentServerDynamicConfig
    : public TNativeSingletonsDynamicConfig
{
public:
    TYqlAgentDynamicConfigPtr YqlAgent;

    REGISTER_YSON_STRUCT(TYqlAgentServerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlAgentServerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
