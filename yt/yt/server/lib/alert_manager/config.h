#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NAlertManager {

////////////////////////////////////////////////////////////////////////////////

struct TAlertManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    TDuration AlertCollectionPeriod;

    REGISTER_YSON_STRUCT(TAlertManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAlertManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager
