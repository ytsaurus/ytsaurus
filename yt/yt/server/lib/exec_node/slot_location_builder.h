#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

struct TSlotConfig
    : public NYTree::TYsonStruct
{
    int Index;

    std::optional<int> Uid;

    REGISTER_YSON_STRUCT(TSlotConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlotConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSlotLocationBuilderConfig
    : public NYTree::TYsonStruct
{
    TString LocationPath;

    int NodeUid;

    std::vector<TSlotConfigPtr> SlotConfigs;

    REGISTER_YSON_STRUCT(TSlotLocationBuilderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlotLocationBuilderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSlotLocationBuilderTool
{
    void operator()(const TSlotLocationBuilderConfigPtr& arg) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
