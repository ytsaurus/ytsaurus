// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "config.h"

#include <yt/yt/orm/server/access_control/config.h>

#include <yt/yt/orm/server/objects/config.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr TMasterConfig::GetObjectManagerConfig() const
{
    return ObjectManager;
}

NYT::NOrm::NServer::NAccessControl::TAccessControlManagerConfigPtr TMasterConfig::GetAccessControlManagerConfig() const
{
    return AccessControlManager;
}

void TMasterConfig::Register(TRegistrar registrar)
{
    NYT::NOrm::NServer::NMaster::TMasterConfig::DoRegister(registrar);

    registrar.UnrecognizedStrategy(NYT::NYTree::EUnrecognizedStrategy::KeepRecursive);

    registrar.Preprocessor([] (TThis* config) {
        config->DBName = DefaultDBName;
        config->ProtobufInterop->DefaultEnumYsonStorageType = NYT::NYson::EEnumYsonStorageType::Int;
    });

    registrar.Parameter("object_manager", &TThis::ObjectManager)
        .DefaultNew();
    registrar.Parameter("access_control_manager", &TThis::AccessControlManager)
        .DefaultNew();
    registrar.Parameter("yt_connector", &TThis::YTConnector)
        .DefaultNew();

    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr TMasterDynamicConfig::GetObjectManagerConfig() const
{
    return ObjectManager;
}

NYT::NOrm::NServer::NAccessControl::TAccessControlManagerConfigPtr TMasterDynamicConfig::GetAccessControlManagerConfig() const
{
    return AccessControlManager;
}

void TMasterDynamicConfig::Register(TRegistrar registrar)
{
    NYT::NOrm::NServer::NMaster::TMasterDynamicConfig::DoRegister(registrar);

    registrar.UnrecognizedStrategy(NYT::NYTree::EUnrecognizedStrategy::KeepRecursive);

    registrar.Preprocessor([] (TThis* config) {
        config->DBName = DefaultDBName;
    });

    registrar.Parameter("object_manager", &TThis::ObjectManager)
        .DefaultNew();
    registrar.Parameter("access_control_manager", &TThis::AccessControlManager)
        .DefaultNew();
    registrar.Parameter("yt_connector", &TThis::YTConnector)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NOrm::NExample::NServer::NLibrary
