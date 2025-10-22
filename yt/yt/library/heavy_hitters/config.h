#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TMisraGriesHeavyHittersConfig
    : public NYTree::TYsonStruct
{
    bool Enable;

    TDuration Window;
    double Threshold;
    i64 DefaultLimit;

    REGISTER_YSON_STRUCT(TMisraGriesHeavyHittersConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMisraGriesHeavyHittersConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
