#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

class TYqlPluginConfig
    : public NYTree::TYsonStruct
{
public:
    TString MRJobBinary;
    TString UdfDirectory;

    //! Mapping cluster name -> proxy address.
    THashMap<TString, TString> Clusters;
    std::optional<TString> DefaultCluster;

    NYTree::INodePtr OperationAttributes;

    TString YTTokenPath;

    //! Path to libyqlplugin.so. Ignored when built with -DYQL_NATIVE=yes.
    std::optional<TString> YqlPluginSharedLibrary;

    REGISTER_YSON_STRUCT(TYqlPluginConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlPluginConfig)

////////////////////////////////////////////////////////////////////////////////

class TYqlAgentConfig
    : public TYqlPluginConfig
{
public:
    //! Used to create channels to other queue agents.
    NBus::TBusConfigPtr BusClient;

    int YqlThreadCount;

    REGISTER_YSON_STRUCT(TYqlAgentConfig);

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

    REGISTER_YSON_STRUCT(TYqlAgentServerConfig);

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
