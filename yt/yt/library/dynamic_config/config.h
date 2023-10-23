#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDynamicConfig {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicConfigManagerOptions
{
    //! Path to node with dynamic config in Cypress.
    NYPath::TYPath ConfigPath;

    //! Name of the dynamic config manager. Used in logging
    //! and alerts only.
    TString Name;

    //! If true, node with dynamic config contains not just
    //! a config, but a map from boolean formula to dynamic
    //! config. In that case the single config whose formula
    //! is satisfied by instances's tags is used. If multiple
    //! dynamic config formulas are satisfied, none of the
    //! configs are applied and alert is set.
    bool ConfigIsTagged = false;

    //! Type of the master channel that is used for dynamic
    //! config fetching.
    NApi::EMasterChannelKind ReadFrom = NApi::EMasterChannelKind::Cache;
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManagerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period of config fetching from Cypress.
    TDuration UpdatePeriod;

    //! Whether alert for unrecognized dynamic config options
    //! should be enabled.
    bool EnableUnrecognizedOptionsAlert;

    //! If true, config node absence will not be tracted as
    //! an error.
    bool IgnoreConfigAbsence;

    REGISTER_YSON_STRUCT(TDynamicConfigManagerConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDynamicConfig
