#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueryTrackerClient {

///////////////////////////////////////////////////////////////////////////////

class TQueryTrackerStageConfig
    : public NYTree::TYsonStruct
{
public:
    NYPath::TYPath Root;
    TString User;

    REGISTER_YSON_STRUCT(TQueryTrackerStageConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerStageConfig)

///////////////////////////////////////////////////////////////////////////////

class TQueryTrackerConnectionConfig
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TQueryTrackerStageConfigPtr> Stages;

    i64 MaxQueryFileCount;
    i64 MaxQueryFileNameSizeBytes;
    i64 MaxQueryFileContentSizeBytes;

    REGISTER_YSON_STRUCT(TQueryTrackerConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TQueryTrackerConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTrackerClient
