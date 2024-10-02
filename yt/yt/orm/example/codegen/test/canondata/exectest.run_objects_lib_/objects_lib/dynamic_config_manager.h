// AUTOMATICALLY GENERATED. DO NOT EDIT!

#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/orm/server/master/bootstrap.h>

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

//! Manages dynamic configuration by pulling it periodically from Cypress.
/*!
 *  \note
 *  Thread affinity: any
 */
struct IDynamicConfigManager
    : public NYT::NDynamicConfig::TDynamicConfigManagerBase<TMasterDynamicConfig>
{
    using TDynamicConfigManagerBase<TMasterDynamicConfig>::TDynamicConfigManagerBase;
};

DEFINE_REFCOUNTED_TYPE(IDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

IDynamicConfigManagerPtr CreateDynamicConfigManager(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    const TMasterConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
