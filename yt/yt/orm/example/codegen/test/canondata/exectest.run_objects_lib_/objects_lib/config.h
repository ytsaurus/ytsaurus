// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include "public.h"

#include <yt/yt/orm/server/access_control/public.h>

#include <yt/yt/orm/server/objects/public.h>

#include <yt/yt/orm/server/master/config.h>

#include <yt/yt/library/dynamic_config/public.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

inline constexpr char DefaultDBName[] = "example";

class TMasterConfig
    : public NYT::NOrm::NServer::NMaster::TMasterConfig
{
public:
    NYT::TIntrusivePtr<NYT::NOrm::NServer::NObjects::TObjectManagerConfig> ObjectManager;
    NYT::TIntrusivePtr<NYT::NOrm::NServer::NAccessControl::TAccessControlManagerConfig> AccessControlManager;
    NYT::TIntrusivePtr<NYT::NOrm::NServer::NMaster::TDataModelYTConnectorConfig</*AuthenticationRequired*/ true>> YTConnector;

    virtual NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr GetObjectManagerConfig() const override;
    virtual NYT::NOrm::NServer::NAccessControl::TAccessControlManagerConfigPtr GetAccessControlManagerConfig() const override;

    NYT::NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    NYT::NYPath::TYPath DynamicConfigPath;

    REGISTER_YSON_STRUCT(TMasterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterDynamicConfig
    : public NYT::NOrm::NServer::NMaster::TMasterDynamicConfig
{
public:
    NYT::TIntrusivePtr<NYT::NOrm::NServer::NObjects::TObjectManagerConfig> ObjectManager;
    NYT::TIntrusivePtr<NYT::NOrm::NServer::NAccessControl::TAccessControlManagerConfig> AccessControlManager;
    NYT::TIntrusivePtr<NYT::NOrm::NServer::NMaster::TDataModelYTConnectorConfig</*AuthenticationRequired*/ true>> YTConnector;

    virtual NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr GetObjectManagerConfig() const override;
    virtual NYT::NOrm::NServer::NAccessControl::TAccessControlManagerConfigPtr GetAccessControlManagerConfig() const override;

    REGISTER_YSON_STRUCT(TMasterDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
