#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NAlertManager {

////////////////////////////////////////////////////////////////////////////////

class TAlertManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration AlertCollectionPeriod;

    REGISTER_YSON_STRUCT(TAlertManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAlertManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAlertManager
