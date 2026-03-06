#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TMinHashSimilarityConfig
    : public NYTree::TYsonStruct
{
    double MinSimilarity;
    int MinRowCount;

    REGISTER_YSON_STRUCT(TMinHashSimilarityConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMinHashSimilarityConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
